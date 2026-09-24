# stove0_operator_contracts.EvaluationReviewRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationreviewrequest:dbdf6aa5f0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31110e2bdb"></a>
- <a id="s-17a5792d58"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-09f6f09067"></a>`module`: `stove0_operator_contracts`
- <a id="s-2ae8f70aad"></a>`name`: `EvaluationReviewRequest`
- <a id="s-21e1878fc7"></a>`unit`: `export`

### Declared structure

- <a id="s-899ed1d7ba"></a>`kind`: `"class"`
- <a id="s-70faa12576"></a>`signature`: `"\"(*, rating: Annotated[int \| None, Ge(ge=1), Le(le=5)] = None, note: Annotated[Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\\\\\S(?:[\\\\\\\\s\\\\\\\\S]*\\\\\\\\S)?$', ascii_only=None)]], MaxLen(max_length=4000)] = None) -> None\""`

#### Validated model schema

<a id="s-719b709aae"></a>

- <a id="s-676d6ce0db"></a>`type`: `"object"`
- <a id="s-e2cfc8b4f3"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2abbc0a903"></a>`note` | no | anyOf=[(type="string"; maxLength=4000; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$"); (type="null")]; default=null |  |
| <a id="s-a2697b1072"></a>`rating` | no | anyOf=[(type="integer"; minimum=1; maximum=5); (type="null")]; default=null |  |

##### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `anyOf` alternative 1](#s-80207eb499) |
| 2 | [See `anyOf` alternative 2](#s-bdf47442b4) |

##### <a id="s-80207eb499"></a>`anyOf` alternative 1

- <a id="s-8a0bc09eab"></a>`required`: `["rating"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6bf490c281"></a>`rating` | yes | type="integer" |  |

##### <a id="s-bdf47442b4"></a>`anyOf` alternative 2

- <a id="s-f08cdb90b5"></a>`required`: `["note"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8859b052a0"></a>`note` | yes | type="string" |  |

## Maintained corroboration

### Related interface records

- [meaningful](stove0-operator-contracts-evaluationreviewrequest-meaningful.md)

## Governing policies

- <a id="pa-bbd8ee757a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationReviewRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a10b9d7e36fe578c4ab48f73385c39cfc1768245851cc272cc6c852e36fbc952 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "anyOf": [
        {
          "properties": {
            "rating": {
              "type": "integer"
            }
          },
          "required": [
            "rating"
          ]
        },
        {
          "properties": {
            "note": {
              "type": "string"
            }
          },
          "required": [
            "note"
          ]
        }
      ],
      "properties": {
        "note": {
          "anyOf": [
            {
              "maxLength": 4000,
              "minLength": 1,
              "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "rating": {
          "anyOf": [
            {
              "maximum": 5,
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "type": "object"
    },
    "signature": "\"(*, rating: Annotated[int | None, Ge(ge=1), Le(le=5)] = None, note: Annotated[Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\\\\\S(?:[\\\\\\\\s\\\\\\\\S]*\\\\\\\\S)?$', ascii_only=None)]], MaxLen(max_length=4000)] = None) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationReviewRequest",
  "unit": "export"
}
```

</details>
