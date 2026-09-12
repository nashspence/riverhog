# stove0-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-target-protocol:stove0-target:2e0dafdaf7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-target-protocol` |
| Interface | `boundary` |
| Family | `process-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `protocols`: ["stove0-transform-target/v1", "stove0-effect-target/v1"]

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

- `/boundaries/process_extensions/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02bfffccedcfbaecbf2e3cdba9ee66fbd386f15633b9c4c7267ddb3a5fbf27d3 -->

```json
{
  "binding": "http",
  "binding_support": "stove0-target-support",
  "binding_support_role": "reusable_library",
  "contract_owner": "stove0-target-protocol",
  "contract_owner_role": "reusable_library",
  "name": "stove0-target",
  "protocols": [
    "stove0-transform-target/v1",
    "stove0-effect-target/v1"
  ],
  "providers": [
    {
      "distribution": "stove0-nvenc-av1-opus-target",
      "images": [
        "stove0-nvenc-av1-opus-target"
      ]
    },
    {
      "distribution": "stove0-opus-target",
      "images": [
        "stove0-opus-target"
      ]
    },
    {
      "distribution": "stove0-review-materialize-target",
      "images": [
        "stove0-review-materialize-target"
      ]
    },
    {
      "distribution": "stove0-review-rclone-effect-target",
      "images": [
        "stove0-review-rclone-effect-target"
      ]
    }
  ],
  "schema_bundle_format": "stove0-target-schema-bundle/v1"
}
```
