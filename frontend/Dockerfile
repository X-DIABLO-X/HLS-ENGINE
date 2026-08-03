# syntax=docker/dockerfile:1

# -----------------------------------------------------------------------------
# Dependencies stage
# -----------------------------------------------------------------------------
FROM node:22.23.2-alpine AS deps

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci

# -----------------------------------------------------------------------------
# Builder stage
# -----------------------------------------------------------------------------
FROM node:22.23.2-alpine AS builder

WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Build-time env vars; NEXT_PUBLIC_* are baked into the client bundle.
# GATEWAY_URL and CDN_ORIGIN_URL are used by next.config.js rewrites() which are evaluated at build time.
ARG NEXT_PUBLIC_API_BASE_URL=http://localhost/api
ARG NEXT_PUBLIC_HLS_BASE_URL=http://localhost/hls
ARG GATEWAY_URL=http://gateway:8080
ARG CDN_ORIGIN_URL=http://cdn-origin:8080
ENV NEXT_PUBLIC_API_BASE_URL=${NEXT_PUBLIC_API_BASE_URL}
ENV NEXT_PUBLIC_HLS_BASE_URL=${NEXT_PUBLIC_HLS_BASE_URL}
ENV GATEWAY_URL=${GATEWAY_URL}
ENV CDN_ORIGIN_URL=${CDN_ORIGIN_URL}

RUN npm run build

# -----------------------------------------------------------------------------
# Runtime stage
# -----------------------------------------------------------------------------
FROM node:22.23.2-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3000

# Copy only what is needed to run the production Next.js app.
COPY --from=builder /app/public ./public
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget --no-verbose --tries=1 -O /dev/null http://127.0.0.1:3000/ || exit 1

CMD ["node", "server.js"]
