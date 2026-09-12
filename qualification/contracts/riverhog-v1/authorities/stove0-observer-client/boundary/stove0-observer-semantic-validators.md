# stove0.observer-semantic-validators

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-client:stove0-observer-semantic-validators:39aeb2ff90 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-client` |
| Interface | `boundary` |
| Family | `entry-point-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/entry_point_extensions/4`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract

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
