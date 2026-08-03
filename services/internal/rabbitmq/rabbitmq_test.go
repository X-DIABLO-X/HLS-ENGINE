package rabbitmq

import "testing"

func TestDeterministicMessageID(t *testing.T) {
	first := deterministicMessageID("", "upload.completed", []byte(`{"id":"event-1"}`))
	second := deterministicMessageID("", "upload.completed", []byte(`{"id":"event-1"}`))
	if first != second {
		t.Fatalf("same publish produced different IDs: %q != %q", first, second)
	}
	if len(first) != 64 {
		t.Fatalf("expected a SHA-256 hex ID, got length %d", len(first))
	}

	changedBody := deterministicMessageID("", "upload.completed", []byte(`{"id":"event-2"}`))
	if first == changedBody {
		t.Fatal("different message bodies produced the same ID")
	}
	changedRoute := deterministicMessageID("", "other.queue", []byte(`{"id":"event-1"}`))
	if first == changedRoute {
		t.Fatal("different routing keys produced the same ID")
	}
}

func TestRetryDelayIsBoundedExponentialBackoff(t *testing.T) {
	if got := retryDelay(0); got.Milliseconds() != 100 {
		t.Fatalf("first retry delay = %s, want 100ms", got)
	}
	if got := retryDelay(1); got.Milliseconds() != 200 {
		t.Fatalf("second retry delay = %s, want 200ms", got)
	}
}
