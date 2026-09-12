# stove0.observer-semantic-validators

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-client:stove0-observer-semantic-validators:39aeb2ff90 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-client](../index.md) |
| Interface | [boundary](index.md) |
| Family | [entry-point-extensions](index.md#f-5bea80b9f6) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5ab5618c72"></a>
| Field | Shape |
|---|---|
| <a id="s-08e91d604a"></a>`group` | "stove0.observer-semantic-validators" |
| <a id="s-9fa68e11d3"></a>`owner` | "stove0-observer-client" |
| <a id="s-e35eaff2a9"></a>`owner_constant` | "SEMANTIC_VALIDATOR_ENTRY_POINT_GROUP" |
| <a id="s-996b641697"></a>`owner_path` | "reference/stove0/packages/observer-client" |
| <a id="s-066920e27b"></a>`providers` | items=additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` |

## Governing policies

- <a id="pa-d16c9cf59a"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

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
