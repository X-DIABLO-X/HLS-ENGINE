# Security policy

## Supported versions

HLS-ENGINE is currently developed on the default branch and does not yet
maintain multiple security-support release lines.

| Version | Security updates |
|---|---|
| Latest default branch | Supported |
| Older commits, forks, and unmaintained deployments | Not supported |

## Report a vulnerability

Please report suspected vulnerabilities through
[GitHub private vulnerability reporting](https://github.com/X-DIABLO-X/HLS-ENGINE/security/advisories/new).
Do not disclose security details in a public issue, discussion, or pull
request.

Include, where possible:

- The affected commit or version
- A concise description of the vulnerability and its impact
- Reproduction steps that do not contain private media or real credentials
- Relevant configuration, logs, or request traces with secrets removed
- Any known mitigations

Maintainers aim to acknowledge a report within seven calendar days and provide
an initial assessment within fourteen. Fix and disclosure timing depends on
severity, complexity, and release coordination. This project does not
currently offer a bug bounty.

## Good-faith research

When investigating this project:

- Test only systems and data you own or have explicit permission to use.
- Minimize access to data and stop after confirming the issue.
- Avoid privacy violations, service disruption, destructive actions, and
  persistence.
- Give maintainers a reasonable opportunity to remediate before disclosure.

Good-faith reports following these guidelines are appreciated.

## Deployment security

The repository defaults are intended for isolated local development. Before
any network-accessible deployment:

- Replace every `change-me` value and use independent, randomly generated
  secrets.
- Set `NGINX_CACHE_PURGE_SECRET` to a distinct random value of at least 32
  characters. Do not publish the cache-purger port; it is intentionally
  reachable only on the private Compose network and runs as the unprivileged
  Nginx worker user after its bounded cache directories are initialized.
- Terminate TLS in front of Nginx and configure the public API, HLS, and CORS
  origins explicitly.
- Do not expose PostgreSQL, Redis, RabbitMQ, MinIO administration, Prometheus,
  or Grafana directly to the public internet.
- Use least-privilege service accounts for object storage instead of MinIO
  root credentials.
- Restrict account creation and administrative access for the intended
  environment.
- Keep the host, container runtime, images, FFmpeg, language dependencies, and
  browser packages patched.
- Treat uploaded media and its metadata as untrusted input. Apply resource
  limits and isolate media workers from sensitive host paths.
- Back up PostgreSQL and object storage, and test restoration and deletion
  procedures.
- Monitor authentication failures, queue growth, worker restarts, transcoding
  stalls, disk usage, and unexpected object-store access.

Operators remain responsible for the security and legal compliance of their
deployment and media.
