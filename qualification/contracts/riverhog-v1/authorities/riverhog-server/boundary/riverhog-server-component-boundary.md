# riverhog-server component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-server:riverhog-server-component-boundary:ed9704d0e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-server` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`riverhog-api` |
| `dependencies` | ["http-api-contracts","lifecycle-events","riverhog-age","riverhog-application-access","riverhog-archive-contracts","riverhog-protocol","riverhog-provenance","riverhog-provenance-contracts","riverhog-storage-adapter-protocol","riverhog-storage-adapter-support","state-schema","time-formats"] |
| `distribution` | "riverhog-server" |
| `optional_dependencies` | empty object |
| `path` | "riverhog" |
| `role` | "deployed_implementation" |

## Governing policies

- `boundary/frozen-authority/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

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
