# riverhog-server component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-server:riverhog-server-component-boundary:ed9704d0e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-5280ff2862d5) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-edfcd165fb47"></a>
| Field | Shape |
|---|---|
| <a id="s-9dbc8f617a35"></a>`console_scripts` | additional keys=`riverhog-api` |
| <a id="s-1c01268ac6d3"></a>`dependencies` | ["http-api-contracts","lifecycle-events","riverhog-age","riverhog-application-access","riverhog-archive-contracts","riverhog-protocol","riverhog-provenance","riverhog-provenance-contracts","riverhog-storage-adapter-protocol","riverhog-storage-adapter-support","state-schema","time-formats"] |
| <a id="s-d62ae2224dfb"></a>`distribution` | "riverhog-server" |
| <a id="s-252c7ba7fd13"></a>`optional_dependencies` | empty object |
| <a id="s-6322d9c05101"></a>`path` | "riverhog" |
| <a id="s-b68196cf1324"></a>`role` | "deployed_implementation" |

## Governing policies

- <a id="pa-5bf1d2166cb5"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

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
