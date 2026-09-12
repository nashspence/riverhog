# riverhog-storage-adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-protocol:riverhog-storage-adapter:6be6824f2b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-protocol` |
| Interface | `boundary` |
| Family | `process-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/process_extensions/0`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

- `protocols`: ["riverhog-storage-adapter/v1"]

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c726a3fde3874d0a7c71e34a1ba1871105d791ba4445777eb0932b2831a3a7a -->

```json
{
  "binding": "http",
  "binding_support": "riverhog-storage-adapter-support",
  "binding_support_role": "reusable_library",
  "contract_owner": "riverhog-storage-adapter-protocol",
  "contract_owner_role": "reusable_library",
  "name": "riverhog-storage-adapter",
  "protocols": [
    "riverhog-storage-adapter/v1"
  ],
  "providers": [
    {
      "distribution": "riverhog-storage-adapter-aws",
      "images": [
        "riverhog-storage-adapter-aws"
      ]
    },
    {
      "distribution": "riverhog-storage-adapter-backblaze",
      "images": [
        "riverhog-storage-adapter-backblaze"
      ]
    },
    {
      "distribution": "riverhog-storage-adapter-filesystem",
      "images": [
        "riverhog-storage-adapter-filesystem"
      ]
    }
  ],
  "schema_bundle_format": "riverhog-storage-adapter-schema-bundle/v1"
}
```
