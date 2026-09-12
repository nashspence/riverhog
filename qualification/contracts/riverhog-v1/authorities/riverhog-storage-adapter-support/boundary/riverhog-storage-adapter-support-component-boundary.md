# riverhog-storage-adapter-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-support:riverhog-storage-adapter-support-component-boundary:0d1479bc2e -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/12`

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
| `dependencies` | array (2 items) |
| `distribution` | "riverhog-storage-adapter-support" |
| `optional_dependencies` | object (0 fields) |
| `path` | "packages/riverhog-storage-adapter-support" |
| `role` | "reusable_library" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ffe6aa156e5a9ba0b05a14a363d5fc2dd97e7f9b4a2846d53344cff124364b1 -->

```json
{
  "console_scripts": {
    "riverhog-storage-adapter-conformance": "riverhog_storage_adapter_support.conformance:main",
    "riverhog-storage-adapter-schemas": "riverhog_storage_adapter_support.schemas:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-storage-adapter-protocol"
  ],
  "distribution": "riverhog-storage-adapter-support",
  "optional_dependencies": {},
  "path": "packages/riverhog-storage-adapter-support",
  "role": "reusable_library"
}
```
