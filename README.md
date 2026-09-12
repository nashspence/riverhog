# riverhog

Riverhog is a self-hosted archive construction, catalog, transfer, and retrieval system. It
accepts logical collections without staging plaintext on its host, constructs canonical
archive layouts, encrypts them, records collection identity and placement in PostgreSQL, and
coordinates verified archive transfer and retrieval through published storage-adapter
capabilities. Its archives remain independently recoverable with standard tools, without its
service or database. This
repository centers the Riverhog server and generic `riverhog-client` library; nonnormative
reference applications and components integrate through public contracts.

## Contributions

New mechanisms are independently owned and published over Riverhog's public contracts. Checked-in
references form a closed, tightly scoped, maintainer-selected, nonnormative conformance set.
Report suspected vulnerabilities privately through [security reporting](SECURITY.md).

## Start here

Use `make help` for development and validation commands. Use each installed command's
`--help` output for its current interface. A running API publishes its current OpenAPI
document at `/openapi.json`.

## Context

- [Architecture](docs/architecture.md) explains authority, component boundaries, and the
  repository layout.
- [V1 contract atlas](qualification/contracts/riverhog-v1/index.md) provides the generated,
  human-routable view of the exact checked external contract.
- [Licensing](LICENSE.md) defines the repository's release terms.

Release-level reference documentation belongs to tagged releases. The documentation on
`main` is intentionally limited to current context that cannot be recovered quickly from
the executable contracts.
