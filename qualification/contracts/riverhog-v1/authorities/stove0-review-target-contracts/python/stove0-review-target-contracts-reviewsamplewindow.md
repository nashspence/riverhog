# stove0_review_target_contracts.ReviewSampleWindow

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-reviewsamplewindow:980553191b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e272d11c5"></a>
- <a id="s-548d08bb07"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-9c4ed66da6"></a>`module`: `stove0_review_target_contracts`
- <a id="s-2b8c2e73b3"></a>`name`: `ReviewSampleWindow`
- <a id="s-b117aff8b3"></a>`unit`: `export`

### Declared structure

- <a id="s-2365526c11"></a>`kind`: `"class"`
- <a id="s-364b64b8a9"></a>`signature`: `"'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)]) -> None'"`

#### Validated model schema

<a id="s-0aad1ea5a6"></a>

- <a id="s-6fcab1fdf3"></a>`type`: `"object"`
- <a id="s-236d5d6d86"></a>`additionalProperties`: `false`
- <a id="s-e3a5acc937"></a>`required`: `["artifact_id","start_ms","duration_ms"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-35c8ae862c"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-d724965218"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-489c1bc11e"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-5576f89b37"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — [reference/stove0/targets/review/contracts/src/stove0\_review\_target\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.ReviewSampleWindow`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 089be775187822a6308869a5170ce55a7390fdb9a5c8f6f1acd8fe5ef4a85824 -->

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
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "ReviewSampleWindow",
  "unit": "export"
}
```

</details>
