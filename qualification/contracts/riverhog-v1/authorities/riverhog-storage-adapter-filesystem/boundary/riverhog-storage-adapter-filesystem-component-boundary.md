# riverhog-storage-adapter-filesystem component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-compo-09606b0fa7:6c1d9bfa25 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-filesystem` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/38`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `console_scripts` | object (2 fields) |
| `dependencies` | array (3 items) |
| `distribution` | "riverhog-storage-adapter-filesystem" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/riverhog/storage/filesystem" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 886f6b1081eb2cba5d63269ceb08b550402f0a081d46399435e74b1282275d40 -->

```json
{
  "console_scripts": {
    "riverhog-storage-adapter-filesystem": "riverhog_storage_adapter_filesystem.app:main",
    "riverhog-storage-adapter-filesystem-materialize": "riverhog_storage_adapter_filesystem.materialize_cli:main"
  },
  "dependencies": [
    "riverhog-storage-adapter-asgi-support",
    "riverhog-storage-adapter-protocol",
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-filesystem",
  "optional_dependencies": {},
  "path": "reference/riverhog/storage/filesystem",
  "role": "reference_component"
}
```
