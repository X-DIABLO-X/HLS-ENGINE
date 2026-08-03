FROM nginx:1.31.3-alpine

# Install envsubst for substituting environment variables into nginx.conf
RUN apk add --no-cache gettext

# Remove default nginx configuration
RUN rm /etc/nginx/conf.d/default.conf

# Copy custom configuration template
COPY nginx.conf /etc/nginx/nginx.conf.template

# Entrypoint: restore and repair the complete bounded cache tree, substitute
# env vars, and start. A prior root-owned nested cache entry must not make the
# container look healthy while nginx workers cannot update it.
ENTRYPOINT sh -c "mkdir -p /var/cache/nginx/hls && chown -R nginx:nginx /var/cache/nginx/hls && su nginx -s /bin/sh -c 'test -w /var/cache/nginx/hls' && envsubst '\$FRONTEND_HOST \$FRONTEND_PORT \$GATEWAY_HOST \$GATEWAY_PORT \$CDN_ORIGIN_HOST \$CDN_ORIGIN_PORT \$CACHE_PURGER_HOST \$CACHE_PURGER_PORT' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf && exec nginx -g 'daemon off;'"
