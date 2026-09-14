# stove0_target_protocol.TargetProgress

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetprogress:77eee1bcfa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f1971f977"></a>
- <a id="s-4adc55d8d2"></a>`distribution`: `stove0-target-protocol`
- <a id="s-ac8b8cc705"></a>`module`: `stove0_target_protocol`
- <a id="s-b0d75ea25c"></a>`name`: `TargetProgress`
- <a id="s-6b23ec8d9f"></a>`unit`: `export`

### Declared structure

- <a id="s-14eced703b"></a>`kind`: `"class"`
- <a id="s-6828b8e314"></a>`signature`: `"'(*, phase: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], completed: Annotated[int, Ge(ge=0)], total: Annotated[int \| None, Ge(ge=0)] = None, unit: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=40)] = None) -> None'"`

#### Validated model schema

<a id="s-7c961d08d3"></a>
- <a id="s-e911b0aa60"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fff9cc1b56"></a>`completed` | yes | type="integer"; minimum=0 |  |
| <a id="s-61bc69382b"></a>`phase` | yes | type="string"; minLength=1; maxLength=120 |  |
| <a id="s-6907d37959"></a>`total` | no | anyOf=type="integer"; minimum=0 \| type="null" |  |
| <a id="s-b7dfd20adc"></a>`unit` | no | anyOf=type="string"; minLength=1; maxLength=40 \| type="null" |  |

## Maintained corroboration

### Related interface records

- [validate_total](stove0-target-protocol-targetprogress-validate-total.md)

## Governing policies

- <a id="pa-93ba9bb76a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProgress`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a956bb30e27d912484b3826e49aece84d1c41084e9c3d9c5722a8657943241b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "completed": {
          "minimum": 0,
          "type": "integer"
        },
        "phase": {
          "maxLength": 120,
          "minLength": 1,
          "type": "string"
        },
        "total": {
          "anyOf": [
            {
              "minimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "unit": {
          "anyOf": [
            {
              "maxLength": 40,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "phase",
        "completed"
      ],
      "type": "object"
    },
    "signature": "'(*, phase: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], completed: Annotated[int, Ge(ge=0)], total: Annotated[int | None, Ge(ge=0)] = None, unit: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=40)] = None) -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetProgress",
  "unit": "export"
}
```
