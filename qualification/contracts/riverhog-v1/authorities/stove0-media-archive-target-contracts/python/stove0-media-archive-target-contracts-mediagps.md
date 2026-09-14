# stove0_media_archive_target_contracts.MediaGps

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-mediagps:f0d20946c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e6bab8514"></a>
- <a id="s-216497b241"></a>`distribution`: `stove0-media-archive-target-contracts`
- <a id="s-8ad65404e7"></a>`module`: `stove0_media_archive_target_contracts`
- <a id="s-ffc271b039"></a>`name`: `MediaGps`
- <a id="s-3d733cbd5d"></a>`unit`: `export`

### Declared structure

- <a id="s-c477ca050f"></a>`kind`: `"class"`
- <a id="s-6298674ede"></a>`signature`: `"'(*, latitude: float, longitude: float) -> None'"`

#### Validated model schema

<a id="s-378fb1755f"></a>
- <a id="s-aed7a27703"></a>`title`: MediaGps
- <a id="s-4b21b9e118"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-010432088a"></a>`latitude` | yes | type="number" |  |
| <a id="s-f36b47b478"></a>`longitude` | yes | type="number" |  |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_contracts.MediaGps.valid_position](stove0-media-archive-target-contracts-mediagps-valid-position.md)

## Governing policies

- <a id="pa-4308186fcd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaGps`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a09c846588ccd30be4724e1171e4887bb6e36d0fb4bf238cab00de43bbc99b25 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "latitude": {
          "title": "Latitude",
          "type": "number"
        },
        "longitude": {
          "title": "Longitude",
          "type": "number"
        }
      },
      "required": [
        "latitude",
        "longitude"
      ],
      "title": "MediaGps",
      "type": "object"
    },
    "signature": "'(*, latitude: float, longitude: float) -> None'"
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "MediaGps",
  "unit": "export"
}
```
