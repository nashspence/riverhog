# stove0_operator_contracts.EvaluationCreatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationcreatedevent:234180fdf8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a9e93a2755"></a>
- <a id="s-8212001da0"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-6ce32f60bb"></a>`module`: `stove0_operator_contracts`
- <a id="s-a72cde56eb"></a>`name`: `EvaluationCreatedEvent`
- <a id="s-6311f4ff52"></a>`unit`: `export`

### Declared structure

- <a id="s-f3bb6464e6"></a>`kind`: `"class"`
- <a id="s-136e1859ad"></a>`signature`: `"\"(*, id: Annotated[str, MinLen(min_length=1)], type: Literal['io.riverhog.stove0.evaluation.created'], subject: Annotated[str, MinLen(min_length=1)], occurred_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], payload: stove0_operator_contracts.EvaluationCreatedEventData) -> None\""`

#### Validated model schema

<a id="s-23ad66d4b5"></a>

- <a id="s-e3cc3f3417"></a>`type`: `"object"`
- <a id="s-6542e6ff54"></a>`additionalProperties`: `false`
- <a id="s-b468fa870b"></a>`required`: `["id","type","subject","occurred_at","payload"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4ac098cf9a"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-178b478eed"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-d536f650e4"></a>`payload` | yes | [EvaluationCreatedEventData](#s-dbda641bbc) |  |
| <a id="s-4b10b4cd7d"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-e30bee9869"></a>`type` | yes | type="string"; const="io.riverhog.stove0.evaluation.created" |  |

##### Definitions

- [EvaluationCreatedEventData](#s-dbda641bbc)

##### <a id="s-dbda641bbc"></a>definition `EvaluationCreatedEventData`

- <a id="s-dd04f91098"></a>`type`: `"object"`
- <a id="s-6ea2cf0275"></a>`additionalProperties`: `false`
- <a id="s-ff5b5b1823"></a>`required`: `["evaluation_id","phase"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-55ad8eb68d"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1b45f6a394"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"] |  |

## Maintained corroboration

### Related interface records

- [exact_subject](stove0-operator-contracts-evaluationcreatedevent-exact-subject.md)

## Governing policies

- <a id="pa-f745cb5c96"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationCreatedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e5193711ffe78f5ea9ad82b618f06cc54aee81cfc1729eb6f8e9e7f444eff0df -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "EvaluationCreatedEventData": {
          "additionalProperties": false,
          "properties": {
            "evaluation_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "phase": {
              "enum": [
                "planning",
                "running",
                "partially_complete",
                "complete",
                "failed",
                "canceled"
              ],
              "type": "string"
            }
          },
          "required": [
            "evaluation_id",
            "phase"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "id": {
          "minLength": 1,
          "type": "string"
        },
        "occurred_at": {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        "payload": {
          "$ref": "#/$defs/EvaluationCreatedEventData"
        },
        "subject": {
          "minLength": 1,
          "type": "string"
        },
        "type": {
          "const": "io.riverhog.stove0.evaluation.created",
          "type": "string"
        }
      },
      "required": [
        "id",
        "type",
        "subject",
        "occurred_at",
        "payload"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, MinLen(min_length=1)], type: Literal['io.riverhog.stove0.evaluation.created'], subject: Annotated[str, MinLen(min_length=1)], occurred_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], payload: stove0_operator_contracts.EvaluationCreatedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationCreatedEvent",
  "unit": "export"
}
```

</details>
