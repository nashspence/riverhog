# stove0_media_archive_target_contracts.MediaFieldPreference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-med-266e049f42:73f810e28b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e1ceeb836"></a>
- <a id="s-4b72f4ae74"></a>`distribution`: `stove0-media-archive-target-contracts`
- <a id="s-e1e383d29a"></a>`module`: `stove0_media_archive_target_contracts`
- <a id="s-b0e76192b9"></a>`name`: `MediaFieldPreference`
- <a id="s-6095b93a50"></a>`unit`: `export`

### Declared structure

- <a id="s-ff2a2050ed"></a>`kind`: `"class"`
- <a id="s-0d13b7e0ff"></a>`signature`: `"\"(*, name: Literal['capture-time', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], fields: Annotated[tuple[str, ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-e045631091"></a>
- <a id="s-2396937152"></a>`title`: MediaFieldPreference
- <a id="s-334b43331b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2a1b7c8d9"></a>`fields` | yes | type="array"; minItems=1; items=(type="string") |  |
| <a id="s-1e695dce70"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_contracts.MediaFieldPreference.unique_fields](stove0-media-archive-target-contracts-mediafieldpreference-unique-fields.md)

## Governing policies

- <a id="pa-5c562f5f23"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaFieldPreference`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 245906dfb3c1b7adcc60407363862c60077dfaf5bb4086fd5f3d03dfafe9a837 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "fields": {
          "items": {
            "type": "string"
          },
          "minItems": 1,
          "title": "Fields",
          "type": "array"
        },
        "name": {
          "enum": [
            "capture-time",
            "creator",
            "device-make",
            "device-model",
            "gps-latitude",
            "gps-longitude"
          ],
          "title": "Name",
          "type": "string"
        }
      },
      "required": [
        "name",
        "fields"
      ],
      "title": "MediaFieldPreference",
      "type": "object"
    },
    "signature": "\"(*, name: Literal['capture-time', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], fields: Annotated[tuple[str, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "MediaFieldPreference",
  "unit": "export"
}
```
