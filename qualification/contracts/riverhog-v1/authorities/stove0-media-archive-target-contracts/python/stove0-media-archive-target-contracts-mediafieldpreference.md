# stove0_media_archive_target_contracts.MediaFieldPreference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-med-266e049f42:73f810e28b -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-334b43331b"></a>`type`: `"object"`
- <a id="s-b08892a21e"></a>`additionalProperties`: `false`
- <a id="s-dc8e61cedb"></a>`required`: `["name","fields"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2a1b7c8d9"></a>`fields` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-1e695dce70"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |

## Maintained corroboration

### Related interface records

- [unique_fields](stove0-media-archive-target-contracts-mediafieldpreference-unique-fields.md)

## Governing policies

- <a id="pa-5c562f5f23"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources/authorities.md#src-dfeb5229f2) — [some-implementations/stove0/targets/media-archive/contracts/src/stove0\_media\_archive\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaFieldPreference`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 909414ab62984e54488adb69e158090f814ff0f21c954001b5ceb5cf743a107a -->

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
          "type": "string"
        }
      },
      "required": [
        "name",
        "fields"
      ],
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

</details>
