# a_stove0_media_archive_lib.RetainedXmpSidecar

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-retainedxmpsidecar:78ea92d34f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-20ca0aaff5"></a>
- <a id="s-a1a37e679a"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-1709b1826e"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-107c7c7c98"></a>`name`: `RetainedXmpSidecar`
- <a id="s-fc58e2de08"></a>`unit`: `export`

### Declared structure

- <a id="s-3e1fd3a642"></a>`kind`: `"class"`
- <a id="s-e168d405ce"></a>`signature`: `"'(*, input_artifact_id: str, output_path: str) -> None'"`

#### Validated model schema

<a id="s-cbd6a2698f"></a>

- <a id="s-6621d1c142"></a>`type`: `"object"`
- <a id="s-c5a7ece70a"></a>`additionalProperties`: `false`
- <a id="s-dce3b13d48"></a>`required`: `["input_artifact_id","output_path"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a73d24e559"></a>`input_artifact_id` | yes | type="string" |  |
| <a id="s-711572b502"></a>`output_path` | yes | type="string" |  |

## Maintained corroboration

### Related interface records

- [canonical_path](a-stove0-media-archive-lib-retainedxmpsidecar-canonical-path.md)

## Governing policies

- <a id="pa-ccf3a95c90"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.RetainedXmpSidecar`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2348c16ca624dbf8472249ea7c212be1d532ab30c5039b5c8b8e6654b43e4ccb -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "input_artifact_id": {
          "type": "string"
        },
        "output_path": {
          "type": "string"
        }
      },
      "required": [
        "input_artifact_id",
        "output_path"
      ],
      "type": "object"
    },
    "signature": "'(*, input_artifact_id: str, output_path: str) -> None'"
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "RetainedXmpSidecar",
  "unit": "export"
}
```

</details>
