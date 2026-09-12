# riverhog-storage-adapter-backblaze component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-backblaze:riverhog-storage-adapter-backblaze-compon-5a06186e8e:7defe9e1e1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-backblaze` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`riverhog-storage-adapter-backblaze` |
| `dependencies` | ["riverhog-storage-adapter-asgi-support","riverhog-storage-adapter-s3-support"] |
| `distribution` | "riverhog-storage-adapter-backblaze" |
| `optional_dependencies` | empty object |
| `path` | "reference/riverhog/storage/backblaze" |
| `role` | "reference_component" |

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

- `/boundaries/components/37`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95a7f8988b0c86e176800a81be67ab0e489cd28e8b81454ea4c0db6c6543433c -->

```json
{
  "console_scripts": {
    "riverhog-storage-adapter-backblaze": "riverhog_storage_adapter_backblaze.app:main"
  },
  "dependencies": [
    "riverhog-storage-adapter-asgi-support",
    "riverhog-storage-adapter-s3-support"
  ],
  "distribution": "riverhog-storage-adapter-backblaze",
  "optional_dependencies": {},
  "path": "reference/riverhog/storage/backblaze",
  "role": "reference_component"
}
```
