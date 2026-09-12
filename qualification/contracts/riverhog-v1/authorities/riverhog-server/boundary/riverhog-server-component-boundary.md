# riverhog-server component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-server:riverhog-server-component-boundary:ed9704d0e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-5280ff2862) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-edfcd165fb"></a>
| Field | Shape |
|---|---|
| <a id="s-9dbc8f617a"></a>`console_scripts` | additional keys=`riverhog-api` |
| <a id="s-1c01268ac6"></a>`dependencies` | ["http-api-contracts","lifecycle-events","riverhog-age","riverhog-application-access","riverhog-archive-contracts","riverhog-protocol","riverhog-provenance","riverhog-provenance-contracts","riverhog-storage-adapter-protocol","riverhog-storage-adapter-support","state-schema","time-formats"] |
| <a id="s-d62ae2224d"></a>`distribution` | "riverhog-server" |
| <a id="s-252c7ba7fd"></a>`optional_dependencies` | empty object |
| <a id="s-6322d9c051"></a>`path` | "riverhog" |
| <a id="s-b68196cf13"></a>`role` | "deployed_implementation" |

## Governing policies

- <a id="pa-5bf1d2166c"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/70`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8922f3d23d1748ad397c67315d22a83791a5ed78c0b94a7544dcb6c16551d1a -->

```json
{
  "console_scripts": {
    "riverhog-api": "riverhog_api.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "lifecycle-events",
    "riverhog-age",
    "riverhog-application-access",
    "riverhog-archive-contracts",
    "riverhog-protocol",
    "riverhog-provenance",
    "riverhog-provenance-contracts",
    "riverhog-storage-adapter-protocol",
    "riverhog-storage-adapter-support",
    "state-schema",
    "time-formats"
  ],
  "distribution": "riverhog-server",
  "optional_dependencies": {},
  "path": "riverhog",
  "role": "deployed_implementation"
}
```
