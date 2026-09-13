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
| Field | Shape |
|---|---|
| <a id="s-34f473a831"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-303e701b6b"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-c37299f5eb"></a>`module` | "stove0_operator_contracts" |
| <a id="s-7b7d9dbe0e"></a>`name` | "WorkUpdatedEventData" |
| <a id="s-aff6066a24"></a>`unit` | "export" |

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

<!-- exact-contract-value: 0d43095bcd3afa815d9e3ac5d9126efbc024d71d42a0316d2fd1ecf562da1de4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ccdca8ddb59e6f6e51efd841d9b827e5b954d845a51a4146d07e9d1b2d80900a",
    "signature": "\"(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=2)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkUpdatedEventData",
  "unit": "export"
}
```
