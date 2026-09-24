# a_stove0_media_archive_contract_lib.MediaFieldPreference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-media-86cbade978:7369cc4a63 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-999c9e1016"></a>
- <a id="s-599b357bcc"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-4d05b4bc6e"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-b036a31cf7"></a>`name`: `MediaFieldPreference`
- <a id="s-bee580e5ca"></a>`unit`: `export`

### Declared structure

- <a id="s-a13b2771cb"></a>`kind`: `"class"`
- <a id="s-2a01a38583"></a>`signature`: `"\"(*, name: Literal['capture-time', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], fields: Annotated[tuple[str, ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-b85e734796"></a>

- <a id="s-c97ae3536a"></a>`type`: `"object"`
- <a id="s-ad80c96684"></a>`additionalProperties`: `false`
- <a id="s-5b42e98e18"></a>`required`: `["name","fields"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-22b1d1cefd"></a>`fields` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-4cab6bcce1"></a>`name` | yes | type="string"; enum=["capture-time","creator","device-make","device-model","gps-latitude","gps-longitude"] |  |

## Maintained corroboration

### Related interface records

- [unique_fields](a-stove0-media-archive-contract-lib-mediafieldpreference-unique-fields.md)

## Governing policies

- <a id="pa-d25d951111"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.MediaFieldPreference`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f0ed484f71646e4bff8c15194e97834e91e8fff91b3e0e4ce8877296ff653d0 -->

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
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "MediaFieldPreference",
  "unit": "export"
}
```

</details>
