# stove0-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-protocol:stove0-observer:4b992ab432 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-protocol` |
| Interface | `boundary` |
| Family | `process-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `protocols`: ["stove0-content-observer/v1"]

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

- `/boundaries/process_extensions/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1f2b5585a504e73c2a5166cc9541eb0d28f4708324b5531fad9eeb1bc79e941b -->

```json
{
  "binding": "http",
  "binding_support": "stove0-observer-support",
  "binding_support_role": "reusable_library",
  "contract_owner": "stove0-observer-protocol",
  "contract_owner_role": "reusable_library",
  "name": "stove0-observer",
  "protocols": [
    "stove0-content-observer/v1"
  ],
  "providers": [
    {
      "distribution": "stove0-exiftool-observer",
      "images": [
        "stove0-exiftool-observer"
      ]
    },
    {
      "distribution": "stove0-ffprobe-sampling-observer",
      "images": [
        "stove0-ffprobe-sampling-observer"
      ]
    }
  ],
  "schema_bundle_format": "stove0-observer-schema-bundle/v1"
}
```
