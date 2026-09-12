# riverhog-storage-adapter-aws component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-component-boundary:41c3df727d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-aws` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/36`

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
| `console_scripts` | object (1 fields) |
| `dependencies` | array (4 items) |
| `distribution` | "riverhog-storage-adapter-aws" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/riverhog/storage/aws" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 525b75852f9bcc4c78386c910df506a9d41e9dd5dd31c02c1539907405848683 -->

```json
{
  "console_scripts": {
    "riverhog-storage-adapter-aws": "riverhog_storage_adapter_aws.app:main"
  },
  "dependencies": [
    "riverhog-storage-adapter-asgi-support",
    "riverhog-storage-adapter-protocol",
    "riverhog-storage-adapter-s3-support",
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-aws",
  "optional_dependencies": {},
  "path": "reference/riverhog/storage/aws",
  "role": "reference_component"
}
```
