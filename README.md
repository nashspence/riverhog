# riverhog

Riverhog is a self-hosted system for building and using long-term personal archives. It keeps archived content and its recorded provenance independent of storage providers, catalogs, applications, and Riverhog itself, while keeping plaintext and recovery keys under the operator’s control. A complete archive and its separately kept key material can be recovered without a running Riverhog service or database. See [Architecture](docs/architecture.md) for Riverhog's durable system boundaries and [Licensing](LICENSE.md) for release terms.

## Contributions

Extensions should normally be built and released independently against Riverhog's published contracts. Contributing or requesting additional code in this repository is generally not recommended. Report suspected vulnerabilities privately through [security reporting](SECURITY.md).

## Start here

Use `make help` for development and validation commands.

Release reference documentation is bound to immutable releases and published on the [Contract Render](https://nashspence.github.io/riverhog/) website. Documentation on `main` is strictly limited to context that cannot be recovered quickly from executable contracts.
