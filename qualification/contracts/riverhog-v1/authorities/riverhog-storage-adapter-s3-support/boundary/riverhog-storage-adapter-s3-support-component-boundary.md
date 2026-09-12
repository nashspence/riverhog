# riverhog-storage-adapter-s3-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-compo-843346b56b:cbc3e1878a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-s3-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/39`

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
| `console_scripts` | object (0 fields) |
| `dependencies` | array (2 items) |
| `distribution` | "riverhog-storage-adapter-s3-support" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/riverhog/storage/s3-support" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 326d2e4942527c5fbdfb7dc6308303a87688522fe86aa39abb92aa9c9e6ef704 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-storage-adapter-protocol",
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-s3-support",
  "optional_dependencies": {},
  "path": "reference/riverhog/storage/s3-support",
  "role": "reference_component"
}
```
