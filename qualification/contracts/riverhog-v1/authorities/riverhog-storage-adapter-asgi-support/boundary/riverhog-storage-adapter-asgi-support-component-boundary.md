# riverhog-storage-adapter-asgi-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-asgi-support:riverhog-storage-adapter-asgi-support-com-c5256e8bd2:1a3ef4052a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-asgi-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["http-api-contracts","riverhog-storage-adapter-protocol","riverhog-storage-adapter-support"] |
| `distribution` | "riverhog-storage-adapter-asgi-support" |
| `optional_dependencies` | empty object |
| `path` | "packages/riverhog-storage-adapter-asgi-support" |
| `role` | "reusable_library" |

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

- `/boundaries/components/10`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 258b4b5cdeb0997fb68d8c095c0f514cc7e7a5374fd79944b3e85f5593a52204 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "riverhog-storage-adapter-protocol",
    "riverhog-storage-adapter-support"
  ],
  "distribution": "riverhog-storage-adapter-asgi-support",
  "optional_dependencies": {},
  "path": "packages/riverhog-storage-adapter-asgi-support",
  "role": "reusable_library"
}
```
