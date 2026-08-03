/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  output: 'standalone',
  async rewrites() {
    const gatewayUrl = process.env.GATEWAY_URL || 'http://gateway:8080';
    const cdnOriginUrl = process.env.CDN_ORIGIN_URL || 'http://cdn-origin:8080';
    return [
      {
        source: '/api/:path*',
        destination: `${gatewayUrl}/api/:path*`,
      },
      {
        source: '/hls/:path*',
        destination: `${cdnOriginUrl}/hls/:path*`,
      },
    ];
  },
  env: {
    GATEWAY_URL: process.env.GATEWAY_URL || 'http://gateway:8080',
    CDN_ORIGIN_URL: process.env.CDN_ORIGIN_URL || 'http://cdn-origin:8080',
  },
};

module.exports = nextConfig;
