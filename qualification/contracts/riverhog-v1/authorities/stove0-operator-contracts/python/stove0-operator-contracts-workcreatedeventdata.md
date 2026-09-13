# stove0_operator_contracts.WorkCreatedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatedeventdata:c9a2a9e936 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3fea62c41e"></a>
| Field | Shape |
|---|---|
| <a id="s-8d09b2cabd"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6f70aba77d"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-0c742cd251"></a>`module` | "stove0_operator_contracts" |
| <a id="s-55d8088e95"></a>`name` | "WorkCreatedEventData" |
| <a id="s-40e0be8acf"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.WorkCreatedEventData.exact_parent_binding](stove0-operator-contracts-workcreatedeventdata-exact-parent-binding.md)

## Governing policies

- <a id="pa-9fb09b1892"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreatedEventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cec44afcc45b4588baca952d14a9b512b76024e4f8a64a2b1115f28a9f9fd137 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "e1e48763e34ffc2258b2a0ef13e61106e72258ba277b09a9263caad67af1c640",
    "signature": "\"(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'], parent_work_id: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, branch_set_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, join_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkCreatedEventData",
  "unit": "export"
}
```
