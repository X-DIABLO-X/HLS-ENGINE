package rabbitmq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

const (
	publishAttempts = 3
	confirmTimeout  = 10 * time.Second
	connectTimeout  = 3 * time.Second
)

type queueDeclaration struct {
	name    string
	durable bool
}

// Client owns a confirm-enabled AMQP publisher channel. Publishing is
// serialized so each call can unambiguously wait for its broker confirmation.
type Client struct {
	mu            sync.Mutex
	url           string
	conn          *amqp091.Connection
	channel       *amqp091.Channel
	confirmations <-chan amqp091.Confirmation
	returns       <-chan amqp091.Return
	channelClosed <-chan *amqp091.Error
	declarations  map[string]queueDeclaration
	closed        bool
}

func New(url string) (*Client, error) {
	client := &Client{
		url:          url,
		declarations: make(map[string]queueDeclaration),
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if err := client.connectLocked(); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var firstErr error
	if c.channel != nil {
		if err := c.channel.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.channel = nil
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.conn = nil
	}
	return firstErr
}

func (c *Client) DeclareQueue(name string, durable bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("rabbitmq client is closed")
	}
	declaration := queueDeclaration{name: name, durable: durable}
	c.declarations[name] = declaration

	var lastErr error
	for attempt := 0; attempt < publishAttempts; attempt++ {
		if err := c.ensureConnectedLocked(); err != nil {
			lastErr = err
		} else {
			_, err := c.channel.QueueDeclare(name, durable, false, false, false, nil)
			if err == nil {
				return nil
			}
			lastErr = fmt.Errorf("declare queue %q: %w", name, err)
			c.resetLocked()
		}
		if attempt < publishAttempts-1 {
			time.Sleep(retryDelay(attempt))
		}
	}
	return lastErr
}

func (c *Client) PublishJSON(ctx context.Context, exchange, routingKey string, body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("rabbitmq client is closed")
	}

	messageID := deterministicMessageID(exchange, routingKey, body)
	var lastErr error
	for attempt := 0; attempt < publishAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.ensureConnectedLocked(); err != nil {
			lastErr = err
		} else {
			lastErr = c.publishConfirmedLocked(ctx, exchange, routingKey, messageID, body)
			if lastErr == nil {
				return nil
			}
			c.resetLocked()
		}

		if attempt < publishAttempts-1 {
			timer := time.NewTimer(retryDelay(attempt))
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return fmt.Errorf(
		"publish %q to exchange %q was not broker-confirmed after %d attempts: %w",
		routingKey,
		exchange,
		publishAttempts,
		lastErr,
	)
}

func (c *Client) Channel() (*amqp091.Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ensureConnectedLocked(); err != nil {
		return nil, err
	}
	return c.conn.Channel()
}

func (c *Client) Consume(queue, consumer string) (<-chan amqp091.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ensureConnectedLocked(); err != nil {
		return nil, err
	}
	return c.channel.Consume(queue, consumer, false, false, false, false, nil)
}

func (c *Client) publishConfirmedLocked(
	ctx context.Context,
	exchange string,
	routingKey string,
	messageID string,
	body []byte,
) error {
	if err := c.channel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		true,
		false,
		amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			MessageId:    messageID,
			Timestamp:    time.Now().UTC(),
			Body:         body,
		},
	); err != nil {
		return fmt.Errorf("send publish: %w", err)
	}

	timer := time.NewTimer(confirmTimeout)
	defer timer.Stop()
	for {
		select {
		case returned, ok := <-c.returns:
			if !ok {
				return fmt.Errorf("publisher return channel closed")
			}
			return fmt.Errorf(
				"message was unroutable: code=%d text=%q exchange=%q routing_key=%q",
				returned.ReplyCode,
				returned.ReplyText,
				returned.Exchange,
				returned.RoutingKey,
			)
		case confirmation, ok := <-c.confirmations:
			if !ok {
				return fmt.Errorf("publisher confirmation channel closed")
			}
			if !confirmation.Ack {
				return fmt.Errorf("broker negatively acknowledged delivery %d", confirmation.DeliveryTag)
			}
			// RabbitMQ sends basic.return before basic.ack for mandatory,
			// unroutable messages. Check the buffered return listener before
			// accepting the confirmation.
			select {
			case returned, ok := <-c.returns:
				if !ok {
					return fmt.Errorf("publisher return channel closed")
				}
				return fmt.Errorf(
					"message was unroutable: code=%d text=%q exchange=%q routing_key=%q",
					returned.ReplyCode,
					returned.ReplyText,
					returned.Exchange,
					returned.RoutingKey,
				)
			default:
				return nil
			}
		case amqpErr, ok := <-c.channelClosed:
			if !ok || amqpErr == nil {
				return fmt.Errorf("publisher channel closed before confirmation")
			}
			return fmt.Errorf("publisher channel closed before confirmation: %w", amqpErr)
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timed out waiting for publisher confirmation")
		}
	}
}

func (c *Client) ensureConnectedLocked() error {
	if c.closed {
		return fmt.Errorf("rabbitmq client is closed")
	}
	if c.conn != nil && !c.conn.IsClosed() && c.channel != nil && !c.channel.IsClosed() {
		return nil
	}
	c.resetLocked()
	return c.connectLocked()
}

func (c *Client) connectLocked() error {
	conn, err := amqp091.DialConfig(c.url, amqp091.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Dial:      amqp091.DefaultDial(connectTimeout),
	})
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("enable rabbitmq publisher confirms: %w", err)
	}
	confirmations := channel.NotifyPublish(make(chan amqp091.Confirmation, 1))
	returns := channel.NotifyReturn(make(chan amqp091.Return, 1))
	channelClosed := channel.NotifyClose(make(chan *amqp091.Error, 1))

	for _, declaration := range c.declarations {
		if _, err := channel.QueueDeclare(
			declaration.name,
			declaration.durable,
			false,
			false,
			false,
			nil,
		); err != nil {
			_ = channel.Close()
			_ = conn.Close()
			return fmt.Errorf("redeclare queue %q: %w", declaration.name, err)
		}
	}
	c.conn = conn
	c.channel = channel
	c.confirmations = confirmations
	c.returns = returns
	c.channelClosed = channelClosed
	return nil
}

func (c *Client) resetLocked() {
	if c.channel != nil {
		_ = c.channel.Close()
		c.channel = nil
	}
	c.confirmations = nil
	c.returns = nil
	c.channelClosed = nil
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func retryDelay(attempt int) time.Duration {
	return time.Duration(100*(1<<attempt)) * time.Millisecond
}

func deterministicMessageID(exchange, routingKey string, body []byte) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte(exchange))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(routingKey))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(body)
	return hex.EncodeToString(digest.Sum(nil))
}
