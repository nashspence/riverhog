# Riverhog product

[Atlas](../index.md)

Riverhog owns the public archive service and the reusable contracts that define its maintained extension boundaries. Reference implementations remain nonnormative.

## Surface shape

| Semantic area | Exact authorities | Contract elements |
|---|---:|---:|
| Riverhog service | 1 | 471 |
| Riverhog-owned contracts and libraries | 13 | 80 |
| Implementation and build | 5 | 56 |

## Riverhog service

Authorities: **1** · Contract elements: **471**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [riverhog](../authorities/riverhog/index.md) | 471 | http, operation | Encrypted archive management, catalog, and retrieval. |

### Semantic families

| Family | Count |
|---|---:|
| `app-key-access` | 2 |
| `apps` | 18 |
| `archive` | 16 |
| `catalog` | 2 |
| `catalog-sync` | 6 |
| `collection-processing-claims` | 54 |
| `collection-upload-sessions` | 38 |
| `collections` | 39 |
| `download-quota` | 2 |
| `download-quotas` | 2 |
| `events` | 2 |
| `health` | 4 |
| `retrieval-cache` | 6 |
| `retrieval-jobs` | 13 |
| `retrieval-plans` | 8 |
| `schemas` | 253 |
| `search` | 2 |
| `securitySchemes` | 1 |
| `service` | 1 |
| `tags` | 2 |

## Riverhog-owned contracts and libraries

Authorities: **13** · Contract elements: **80**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [http-api-contracts](../authorities/http-api-contracts/index.md) | 2 | boundary, python | Public typed HTTP error, health, client, and operation contracts. |
| [lifecycle-events](../authorities/lifecycle-events/index.md) | 2 | boundary, python | Durable CloudEvents lifecycle log and client primitives. |
| [riverhog-age](../authorities/riverhog-age/index.md) | 2 | boundary, python | Resumable age encryption used by the Riverhog protocol. |
| [riverhog-application-access](../authorities/riverhog-application-access/index.md) | 2 | boundary, python | Public Riverhog application-access contracts and canonical grant grammar. |
| [riverhog-archive-contracts](../authorities/riverhog-archive-contracts/index.md) | 6 | boundary, protocol, python | Dependency-light immutable Riverhog archive recovery contracts. |
| [riverhog-client](../authorities/riverhog-client/index.md) | 15 | boundary, configuration-environment, python | Typed generic Riverhog client and capability-scoped collection-processing runtime. |
| [riverhog-protocol](../authorities/riverhog-protocol/index.md) | 3 | boundary, protocol, python | Canonical Riverhog wire and identity contracts. |
| [riverhog-provenance](../authorities/riverhog-provenance/index.md) | 11 | boundary, configuration-environment, protocol, python | Portable Riverhog v1 per-file provenance journals and validation. |
| [riverhog-provenance-contracts](../authorities/riverhog-provenance-contracts/index.md) | 3 | boundary, python | Canonical Riverhog provenance identity and reference contracts. |
| [riverhog-provenance-installation](../authorities/riverhog-provenance-installation/index.md) | 1 | durable-state | Portable Riverhog v1 per-file provenance journals and validation. |
| [riverhog-storage-adapter-asgi-support](../authorities/riverhog-storage-adapter-asgi-support/index.md) | 2 | boundary, python | Authenticated ASGI shell for independently scoped Riverhog storage adapters. |
| [riverhog-storage-adapter-protocol](../authorities/riverhog-storage-adapter-protocol/index.md) | 3 | boundary, python | Provider-neutral opaque-object capability contracts for Riverhog storage adapters. |
| [riverhog-storage-adapter-support](../authorities/riverhog-storage-adapter-support/index.md) | 28 | boundary, cli, protocol, python | HTTP binding and conformance support for Riverhog storage adapters. |

## Extension boundaries

- [riverhog-storage-adapter](../evidence/relationships.md#rn-994e13bf1e)
  - Independently deployed process protocol owned by riverhog-storage-adapter-protocol.
  - Owner: [riverhog-storage-adapter-protocol](../authorities/riverhog-storage-adapter-protocol/index.md)
- [riverhog.provenance-contracts](../evidence/relationships.md#rn-0855733167)
  - Entry-point extension boundary owned by riverhog-provenance-contracts.
  - Owner: [riverhog-provenance-contracts](../authorities/riverhog-provenance-contracts/index.md)
- [riverhog.provenance-observers](../evidence/relationships.md#rn-ed2bbc0e31)
  - Entry-point extension boundary owned by riverhog-provenance.
  - Owner: [riverhog-provenance](../authorities/riverhog-provenance/index.md)

## Implementation and build

Authorities: **5** · Contract elements: **56**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [config-validation](../authorities/config-validation/index.md) | 1 | boundary | Strict YAML and JSON Schema configuration validation. |
| [riverhog-catalog](../authorities/riverhog-catalog/index.md) | 1 | durable-state | Encrypted archive management, catalog, and retrieval. |
| [riverhog-server](../authorities/riverhog-server/index.md) | 52 | boundary, configuration-environment | Encrypted archive management, catalog, and retrieval. |
| [state-schema](../authorities/state-schema/index.md) | 1 | boundary | Forward-only relational state schema and migration contracts. |
| [time-formats](../authorities/time-formats/index.md) | 1 | boundary | UTC timestamp and operator duration formats. |
