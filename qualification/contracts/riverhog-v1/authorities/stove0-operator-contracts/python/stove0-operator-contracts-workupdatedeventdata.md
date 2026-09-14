# stove0_operator_contracts.WorkUpdatedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workupdatedeventdata:9b2a88ca93 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7b35c832d5"></a>
- <a id="s-303e701b6b"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-c37299f5eb"></a>`module`: `stove0_operator_contracts`
- <a id="s-7b7d9dbe0e"></a>`name`: `WorkUpdatedEventData`
- <a id="s-aff6066a24"></a>`unit`: `export`

### Declared structure

- <a id="s-9b141eb126"></a>`kind`: `"class"`
- <a id="s-28e16cd249"></a>`signature`: `"\"(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=2)]) -> None\""`

#### Validated model schema

<a id="s-9ffc16ff53"></a>
- <a id="s-9559dcd8b0"></a>`title`: WorkUpdatedEventData
- <a id="s-535e4ffd18"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e96b194a5e"></a>`phase` | yes | type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"] |  |
| <a id="s-2ffc6c8240"></a>`revision` | yes | type="integer"; minimum=2 |  |
| <a id="s-dc330eb4cb"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-89465753c1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkUpdatedEventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5e95fe91094b5d4737758a7bccf78446e6b586202d7a1bd324a3d79a3a332f53 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "phase": {
          "enum": [
            "eligible",
            "claimed",
            "observing",
            "planning",
            "target_preflight",
            "queued",
            "executing",
            "output_finalizing",
            "verifying",
            "settled",
            "retirement_pending",
            "coordinating",
            "abandon_pending",
            "complete",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "title": "Phase",
          "type": "string"
        },
        "revision": {
          "minimum": 2,
          "title": "Revision",
          "type": "integer"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Id",
          "type": "string"
        }
      },
      "required": [
        "work_id",
        "phase",
        "revision"
      ],
      "title": "WorkUpdatedEventData",
      "type": "object"
    },
    "signature": "\"(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=2)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkUpdatedEventData",
  "unit": "export"
}
```
