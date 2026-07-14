# Kroxylicious JUnit5 Extension - Claude Context

This is Kroxylicious Junit5 Extension, an extension for JUnit 5 that simplifies spinnig up temporary Kafka clusters for use in tests.

## Quick Reference

- See [README.md](README.md) for usage examples, constraint annotations, and test patterns
- See [DEV_GUIDE.md](DEV_GUIDE.md) for build commands, prerequisites, and Podman setup

## Architecture & Design Patterns

**Service Provider Interface (SPI)**
- `KafkaClusterProvisioningStrategy` implementations loaded via `ServiceLoader`
- Strategy selection based on: constraint annotation support, type compatibility, estimated provisioning time
- Register custom strategies in `META-INF/services/io.kroxylicious.testing.kafka.api.KafkaClusterProvisioningStrategy`

**JUnit5 Extension Integration**
- Resource lifecycle managed via `ExtensionContext.Store` with `Closeable` wrappers for automatic cleanup
- Template test support: `@DimensionMethodSource` (cartesian product), `@ConstraintsMethodSource` (tuples)

**Cluster Naming & Resolution**
- UUID-based internal naming for cluster disambiguation
- `@Name` annotation allows explicit cluster identification when multiple clusters in scope
- Resolution follows "closest enclosing scope" semantics

## Module Responsibilities

```
api/                    # KafkaCluster interface, SPI contracts, @KafkaClusterConstraint meta-annotation
impl/                   # InVM & Testcontainers provisioning, KafkaClusterConfig builders, utilities
junit5-extension/       # JUnit5 extension logic, constraint annotations, field/parameter injection
integration-test/       # End-to-end tests demonstrating usage patterns
```

## Code Patterns

**Extending Provisioning**
- Implement `KafkaClusterProvisioningStrategy` interface
- Register via `META-INF/services` file
- Override `estimatedProvisioningTimeMs()` to influence strategy selection

**Custom Constraints**
- Annotate with `@KafkaClusterConstraint` meta-annotation
- Strategy must declare support via `supportsType()` and `supportsConstraint()`

**Field Injection Safety**
- Extension only injects fields with: no annotations OR annotations from safe packages (`io.kroxylicious`, `org.junit`, `java.lang`)
- Prevents conflicts with other extensions (e.g., Mockito `@Mock`)

**Resource Management**
- Always wrap resources in `Closeable` for automatic cleanup
- Instance fields = per-test clusters; static fields = shared clusters across test class

**Testing Patterns**
- Use `ConstraintUtils` for programmatic constraint creation in test templates
- Leverage `Utils` class for cluster state verification and topology helpers

## Development Constraints

- **Code Formatting**: Run `mvn process-sources` before opening PRs (see [DEV_GUIDE.md](DEV_GUIDE.md))
- **DCO Signoff**: All commits require `Signed-off-by:` trailer (auto-added by git hook)
- **Dependencies**: Kafka/ZooKeeper are `provided` scope - users supply specific versions (Kafka 3.3.0+, ZooKeeper 3.6.3+)
- **Podman**: Requires `service_timeout=91` workaround for testcontainers compatibility (see [DEV_GUIDE.md](DEV_GUIDE.md))

## Commit Conventions

Use this as a template for commit messages:

```
<type>(<scope>): <summary>

<body>

Assisted-by: Claude <model-name> <noreply@anthropic.com>
Signed-off-by: <name> <email>
```

**Commit Summary Format:**
- Use Conventional Commits: `<type>(<scope>): <summary>`
- Types: `feat`, `fix`, `docs`, `build`, `chore`, `refactor`, `perf`, `test`, `ci`
- Keep subject line under 72 characters

**AI Disclosure Requirement:**
When AI assists with code changes, add an `Assisted-by:` trailer:
- Format: `Assisted-by: Claude <model-name> <noreply@anthropic.com>`
- Model name examples: "Claude Sonnet 4.5", "Claude Opus 4.6"
- DO NOT use `Co-Authored-By:`
- Placement: After commit body, before `Signed-off-by:` trailer

**DCO Signoff:**
All commits require `Signed-off-by:` trailer (auto-added by git hook).


