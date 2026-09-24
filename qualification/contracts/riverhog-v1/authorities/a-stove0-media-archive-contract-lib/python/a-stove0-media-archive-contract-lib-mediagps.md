# a_stove0_media_archive_contract_lib.MediaGps

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-mediagps:026539f9a8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2d098ecd26"></a>
- <a id="s-d90a431256"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-7fabe27dce"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-3b51a85acd"></a>`name`: `MediaGps`
- <a id="s-2a469c0792"></a>`unit`: `export`

### Declared structure

- <a id="s-8774c75a92"></a>`kind`: `"class"`
- <a id="s-971c2f0434"></a>`signature`: `"'(*, latitude: float, longitude: float) -> None'"`

#### Validated model schema

<a id="s-6e6e78cf44"></a>

- <a id="s-3faa1081ba"></a>`type`: `"object"`
- <a id="s-4aa77f6e50"></a>`additionalProperties`: `false`
- <a id="s-e8ac719566"></a>`required`: `["latitude","longitude"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3930ee2dc4"></a>`latitude` | yes | type="number" |  |
| <a id="s-c042872377"></a>`longitude` | yes | type="number" |  |

## Maintained corroboration

### Related interface records

- [valid_position](a-stove0-media-archive-contract-lib-mediagps-valid-position.md)

## Governing policies

- <a id="pa-30d1ba0314"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.MediaGps`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b429298975c2165640e6fcfc720addd6a32f4b4949840fe36c9860b7b6070c21 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "latitude": {
          "type": "number"
        },
        "longitude": {
          "type": "number"
        }
      },
      "required": [
        "latitude",
        "longitude"
      ],
      "type": "object"
    },
    "signature": "'(*, latitude: float, longitude: float) -> None'"
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "MediaGps",
  "unit": "export"
}
```

</details>
