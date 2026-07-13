# Kroxylicious JUnit5 Extension - Claude Context

This is Kroxylicious Junit5 Extension, an extension for JUnit 5 that simplifies spinnig up temporary Kafka clusters for use in tests.

## Commit Conventions

When creating commits, follow these conventions:

**Commit Message Format:**
- Use Conventional Commits: `<type>(<scope>): <description>`
- Types: `feat`, `fix`, `docs`, `build`, `chore`, `refactor`, `perf`, `test`, `ci`
- Keep subject line under 72 characters

**AI Disclosure Requirement:**
When AI assists with code changes, add an `Assisted-by:` trailer:
- Format: `Assisted-by: Claude <model-name> <noreply@anthropic.com>`
- Placement: After commit body, before `Signed-off-by:` trailer
- Model name examples: "Claude Sonnet 4.5", "Claude Opus 4.6"

**DCO Signoff:**
All commits require `Signed-off-by:` trailer (auto-added by git hook).

**IMPORTANT for Claude Code:** Use `Assisted-by:` NOT `Co-Authored-By:`. Format:

```
<type>(<scope>): <subject>

<body>

Assisted-by: Claude <model-name> <noreply@anthropic.com>
Signed-off-by: <name> <email>
```

**Example:**
```
feat(filters): add request throttling filter

Implements configurable rate limiting at the filter level with
per-client quotas and burst handling.

Assisted-by: Claude Sonnet 4.5 <noreply@anthropic.com>
Signed-off-by: Jane Developer <jane@example.com>
```


