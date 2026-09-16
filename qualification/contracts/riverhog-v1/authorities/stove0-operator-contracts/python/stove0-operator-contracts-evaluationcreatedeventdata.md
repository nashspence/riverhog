# stove0_operator_contracts.EvaluationCreatedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationcreatedeventdata:04246e15b1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-674496c0ef"></a>
- <a id="s-be23190b94"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-59e18ca097"></a>`module`: `stove0_operator_contracts`
- <a id="s-773daf55cd"></a>`name`: `EvaluationCreatedEventData`
- <a id="s-b4174158a4"></a>`unit`: `export`

### Declared structure

- <a id="s-560311c90a"></a>`kind`: `"class"`
- <a id="s-676f5cd61c"></a>`signature`: `"\"(*, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled']) -> None\""`

#### Validated model schema

<a id="s-2a88a0c022"></a>

- <a id="s-1013824684"></a>`type`: `"object"`
- <a id="s-d359137b0d"></a>`additionalProperties`: `false`
- <a id="s-97844fb4a5"></a>`required`: `["evaluation_id","phase"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d785e8f4ff"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9a8ad20a4c"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"] |  |

## Maintained corroboration

### Related interface records

- [__getitem__](stove0-operator-contracts-evaluationcreatedeventdata-getitem.md)
- [get](stove0-operator-contracts-evaluationcreatedeventdata-get.md)

## Governing policies

- <a id="pa-6edce65f56"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationCreatedEventData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5288e1279d542dd7f230edd11d2762e942d1e99a12ef181ea4a1246d707b4668 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
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
    },
    "signature": "\"(*, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled']) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationCreatedEventData",
  "unit": "export"
}
```

</details>
