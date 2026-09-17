# stove0_operator_contracts.EvaluationReviewIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationreviewin:7a73328a72 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b249a0d91"></a>
- <a id="s-94de2d6c30"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-d5f979f1e9"></a>`module`: `stove0_operator_contracts`
- <a id="s-ee33da7404"></a>`name`: `EvaluationReviewIn`
- <a id="s-af4b6314cf"></a>`unit`: `export`

### Declared structure

- <a id="s-ee74c3ad2e"></a>`kind`: `"class"`
- <a id="s-749736db76"></a>`signature`: `"\"(*, rating: Annotated[int \| None, Ge(ge=1), Le(le=5)] = None, note: Annotated[Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\\\\\S(?:[\\\\\\\\s\\\\\\\\S]*\\\\\\\\S)?$', ascii_only=None)]], MaxLen(max_length=4000)] = None) -> None\""`

#### Validated model schema

<a id="s-c3c9c96bae"></a>

- <a id="s-d3100af1b2"></a>`type`: `"object"`
- <a id="s-ee18bef1a6"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8f6ed07d62"></a>`note` | no | anyOf=[(type="string"; maxLength=4000; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$"); (type="null")]; default=null |  |
| <a id="s-a7f49cdf49"></a>`rating` | no | anyOf=[(type="integer"; minimum=1; maximum=5); (type="null")]; default=null |  |

##### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `anyOf` alternative 1](#s-701da37c1d) |
| 2 | [See `anyOf` alternative 2](#s-c02aa6fa29) |

##### <a id="s-701da37c1d"></a>`anyOf` alternative 1

- <a id="s-8f29836679"></a>`required`: `["rating"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1bbe283d07"></a>`rating` | yes | type="integer" |  |

##### <a id="s-c02aa6fa29"></a>`anyOf` alternative 2

- <a id="s-68bad54edf"></a>`required`: `["note"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c603fa7fe2"></a>`note` | yes | type="string" |  |

## Maintained corroboration

### Related interface records

- [meaningful](stove0-operator-contracts-evaluationreviewin-meaningful.md)

## Governing policies

- <a id="pa-e62ff7e6b4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationReviewIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1bd4e709433c9d07c8a989ca98e730b748e5e8e4ff2451ab93aa34562ccfaaf9 -->

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
  "name": "EvaluationReviewIn",
  "unit": "export"
}
```

</details>
