# stove0.observer-semantic-validators

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-client:stove0-observer-semantic-validators:39aeb2ff90 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-client` |
| Interface | `boundary` |
| Family | `entry-point-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `group` | "stove0.observer-semantic-validators" |
| `owner` | "stove0-observer-client" |
| `owner_constant` | "SEMANTIC_VALIDATOR_ENTRY_POINT_GROUP" |
| `owner_path` | "reference/stove0/packages/observer-client" |
| `providers` | items=additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` |

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

- `/boundaries/entry_point_extensions/4`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a8fe8bdaba3fb926a4aec91fa0096c0f94a3d9e4e4e85c7cbf9ede2b8f4feb5c -->

```json
{
  "group": "stove0.observer-semantic-validators",
  "owner": "stove0-observer-client",
  "owner_constant": "SEMANTIC_VALIDATOR_ENTRY_POINT_GROUP",
  "owner_path": "reference/stove0/packages/observer-client",
  "providers": [
    {
      "distribution": "stove0-media-metadata-observer-contracts",
      "name": "media-metadata",
      "value": "stove0_media_metadata_observer_contracts:MEDIA_METADATA_SEMANTIC_VALIDATOR"
    },
    {
      "distribution": "stove0-media-sampling-observer-contracts",
      "name": "media-sampling",
      "value": "stove0_media_sampling_observer_contracts:MEDIA_SAMPLING_SEMANTIC_VALIDATOR"
    }
  ]
}
```
