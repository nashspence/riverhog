# a_stove0_media_sampling_contract_lib.SampleableRange

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-sampling-contract-lib:a-stove0-media-sampling-contract-lib-sampleablerange:42a5c0e69e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-sampling-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5365990d7f"></a>
- <a id="s-9cb84fd62d"></a>`distribution`: `a-stove0-media-sampling-contract-lib`
- <a id="s-20c54db8ef"></a>`module`: `a_stove0_media_sampling_contract_lib`
- <a id="s-62675fc630"></a>`name`: `SampleableRange`
- <a id="s-97d472ef06"></a>`unit`: `export`

### Declared structure

- <a id="s-132ae0310a"></a>`kind`: `"class"`
- <a id="s-917b47c84c"></a>`signature`: `"'(*, start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)]) -> None'"`

#### Validated model schema

<a id="s-81cb5f1fb9"></a>

- <a id="s-71a56628f3"></a>`type`: `"object"`
- <a id="s-c01032e763"></a>`additionalProperties`: `false`
- <a id="s-bcdd2d8f53"></a>`required`: `["start_ms","duration_ms"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bf1468fa64"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-fb462f2420"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-77db899d37"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-sampling-contract-lib:a_stove0_media_sampling_contract_lib](../../../evidence/sources/authorities.md#src-68256911b9) — [some-implementations/stove0/observers/contracts/media-sampling/src/a\_stove0\_media\_sampling\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-sampling/src/a_stove0_media_sampling_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_sampling_contract_lib.SampleableRange`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e156b351d5ba64556df74fad3c9d6df1f08866561c9b320b9ce22c5486fb062a -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "duration_ms": {
          "minimum": 1,
          "type": "integer"
        },
        "start_ms": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "start_ms",
        "duration_ms"
      ],
      "type": "object"
    },
    "signature": "'(*, start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "a-stove0-media-sampling-contract-lib",
  "module": "a_stove0_media_sampling_contract_lib",
  "name": "SampleableRange",
  "unit": "export"
}
```

</details>
