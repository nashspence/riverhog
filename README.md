# riverhog

Riverhog is a self-hosted archive construction, catalog, transfer, and retrieval system. It
accepts logical collections without staging plaintext on its host, constructs canonical
archive layouts, encrypts them, records collection identity and placement in PostgreSQL, and
coordinates verified archive transfer and retrieval through published storage-adapter
capabilities. Its archives remain independently recoverable with standard tools, without its
service or database. This repository centers the Riverhog server and generic
`riverhog-client` library; applications and components in `some-implementations/`
integrate through public contracts.

## Contributions

Extensions should normally be built and released independently against Riverhog's
published contracts. Contributing or requesting additional code in this repository is
generally not recommended. Report suspected vulnerabilities privately through
[security reporting](SECURITY.md).

## Start here

Use `make help` for development and validation commands. Use each installed command's
`--help` output for its current interface. A running API publishes its current OpenAPI
document at `/openapi.json`.

## Context

- [Architecture](docs/architecture.md) explains authority, component boundaries, and the
  repository layout.
- [V1 contract candidate](https://nashspence.github.io/riverhog/contract-candidate/riverhog-v1/)
  presents the checked Contract Closure with an optional Audit Mode. Its
  [source files](qualification/contracts/riverhog-v1/index.html) are reviewed on `main`.
- [Licensing](LICENSE.md) defines the repository's release terms.

Release-level reference documentation belongs to tagged releases. The documentation on
`main` is intentionally limited to current context that cannot be recovered quickly from
the executable contracts.
