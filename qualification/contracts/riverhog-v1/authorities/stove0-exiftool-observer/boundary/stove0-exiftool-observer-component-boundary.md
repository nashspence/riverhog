# stove0-exiftool-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-exiftool-observer:stove0-exiftool-observer-component-boundary:351c5bf99b -->

| Audit field | Value |
|---|---|
| Authority | `stove0-exiftool-observer` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/44`

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
| `distribution` | "stove0-exiftool-observer" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/observers/exiftool" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 761f99f35b6dce0d9e7f34864664c64de99867f2e86b08c70fdbfbe48c0d8ec0 -->

```json
{
  "console_scripts": {
    "stove0-exiftool-observer": "stove0_exiftool_observer.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "stove0-media-metadata-observer-contracts",
    "stove0-observer-protocol",
    "stove0-observer-support"
  ],
  "distribution": "stove0-exiftool-observer",
  "optional_dependencies": {},
  "path": "reference/stove0/observers/exiftool",
  "role": "reference_component"
}
```
