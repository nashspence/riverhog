# review0_target_contracts.ReviewSampleWindow

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-reviewsamplewindow:bd86d3cb11 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a0f49f955a"></a>
- <a id="s-be634520be"></a>`distribution`: `review0-target-contracts`
- <a id="s-f1aea22183"></a>`module`: `review0_target_contracts`
- <a id="s-76b4781f41"></a>`name`: `ReviewSampleWindow`
- <a id="s-7b7e40103e"></a>`unit`: `export`

### Declared structure

- <a id="s-3f2e0f46aa"></a>`kind`: `"class"`
- <a id="s-977bf88a6f"></a>`signature`: `"'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)]) -> None'"`

#### Validated model schema

<a id="s-8b0a67bdce"></a>

- <a id="s-e6640cb485"></a>`type`: `"object"`
- <a id="s-b45ce7fd20"></a>`additionalProperties`: `false`
- <a id="s-ebc2738bb3"></a>`required`: `["artifact_id","start_ms","duration_ms"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b95bbfd462"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-2233b64ba6"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-6e89a37935"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-e0bdedebf4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.ReviewSampleWindow`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f52ec2bd94535fcc44f24cc12b169c20e2ecd59ca43279ebe09eb1ec61eb6e68 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "artifact_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
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
        "artifact_id",
        "start_ms",
        "duration_ms"
      ],
      "type": "object"
    },
    "signature": "'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "ReviewSampleWindow",
  "unit": "export"
}
```

</details>
