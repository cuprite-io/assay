# Security Policy

## Supported Versions

We currently support and provide security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| v0.x.x  | :white_check_mark: |

## Reporting a Vulnerability

We take the security of Assay seriously. If you believe you have found a security vulnerability, please do not open a public issue or Pull Request. Doing so exposes all active users to zero-day exploits.

Instead, please report vulnerabilities privately:
1. Navigate to the **Security and quality** tab of this repository on GitHub.
2. Click **Advisories**, then **New draft security advisory**.
3. Provide a detailed description, proof of concept, and impact details.

We follow coordinated disclosure and will work with you to patch the issue privately before releasing a public security advisory.

## Built-in Security Features

Assay is designed to safely parse untrusted data directly in the ingestion hot-path:
- **Cardinality Explosion Mitigation**: Caps the maximum number of unique path statistics tracked per schema (`Config.MaxPaths`) to prevent malicious payloads from causing Out-Of-Memory (OOM) failures.
- **Stack Overflow Prevention**: Enforces recursion depth checks (`Config.MaxDepth`) during JSON and reflection parsing to block deeply nested nesting-overflow attacks.
- **PII / Data Privacy Separation**: Discards value data immediately after mapping its type index. Raw payload values are never stored or logged in statistics.
