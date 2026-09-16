# Riverhog product

[Atlas](../index.md)

Riverhog owns the public archive service and the reusable contracts that define its maintained extension boundaries. Reference implementations remain nonnormative.

## Surface shape

| Semantic area | Exact authorities | Contract elements |
|---|---:|---:|
| Riverhog service | 1 | 364 |
| Riverhog-owned contracts and libraries | 13 | 1191 |
| Implementation and build | 3 | 206 |

## Riverhog service

Authorities: **1** · Contract elements: **364**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [riverhog](../authorities/riverhog/index.md) | 364 | HTTP Operations, HTTP Schemas, HTTP Service Declaration, HTTP Security Schemes | Encrypted archive management, catalog, and retrieval. |

## Riverhog-owned contracts and libraries

Authorities: **13** · Contract elements: **1191**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [http-api-contracts](../authorities/http-api-contracts/index.md) | 63 | Python | Public typed HTTP error, health, client, and operation contracts. |
| [lifecycle-events](../authorities/lifecycle-events/index.md) | 26 | Python | Durable CloudEvents lifecycle log and client primitives. |
| [riverhog-age](../authorities/riverhog-age/index.md) | 34 | Python | Resumable age encryption used by the Riverhog protocol. |
| [riverhog-application-access](../authorities/riverhog-application-access/index.md) | 47 | Python | Public Riverhog application-access contracts and canonical grant grammar. |
| [riverhog-archive-contracts](../authorities/riverhog-archive-contracts/index.md) | 76 | Schemas, Python | Dependency-light immutable Riverhog archive recovery contracts. |
| [riverhog-client](../authorities/riverhog-client/index.md) | 281 | Configuration Environment, Python | Typed generic Riverhog client and capability-scoped collection-processing runtime. |
| [riverhog-protocol](../authorities/riverhog-protocol/index.md) | 324 | Schemas, Python | Canonical Riverhog wire and identity contracts. |
| [riverhog-provenance](../authorities/riverhog-provenance/index.md) | 115 | Schemas, Configuration Environment, Python | Portable Riverhog v1 per-file provenance journals and validation. |
| [riverhog-provenance-contracts](../authorities/riverhog-provenance-contracts/index.md) | 16 | Python | Canonical Riverhog provenance identity and reference contracts. |
| [riverhog-provenance-installation](../authorities/riverhog-provenance-installation/index.md) | 1 | Durable State | Portable Riverhog v1 per-file provenance journals and validation. |
| [riverhog-storage-adapter-asgi-support](../authorities/riverhog-storage-adapter-asgi-support/index.md) | 1 | Python | Authenticated ASGI shell for independently scoped Riverhog storage adapters. |
| [riverhog-storage-adapter-protocol](../authorities/riverhog-storage-adapter-protocol/index.md) | 121 | Python | Provider-neutral opaque-object capability contracts for Riverhog storage adapters. |
| [riverhog-storage-adapter-support](../authorities/riverhog-storage-adapter-support/index.md) | 86 | Process Protocol, Process Protocol Operations, Process Protocol Schemas, CLI, Python | HTTP binding and conformance support for Riverhog storage adapters. |

## Implementation and build

Authorities: **3** · Contract elements: **206**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [riverhog-catalog](../authorities/riverhog-catalog/index.md) | 91 | Durable State | Encrypted archive management, catalog, and retrieval. |
| [riverhog-server](../authorities/riverhog-server/index.md) | 57 | CLI, Configuration Environment | Encrypted archive management, catalog, and retrieval. |
| [state-schema](../authorities/state-schema/index.md) | 58 | Python | Forward-only relational state schema and migration contracts. |
