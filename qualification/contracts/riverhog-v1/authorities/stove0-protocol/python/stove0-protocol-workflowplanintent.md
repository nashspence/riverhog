# stove0_protocol.WorkflowPlanIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplanintent:0cac2ec3e5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f4992aa06"></a>
| Field | Shape |
|---|---|
| <a id="s-fe306dca75"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7e6f18d68f"></a>`distribution` | "stove0-protocol" |
| <a id="s-7c44eb7e20"></a>`module` | "stove0_protocol" |
| <a id="s-2f3063eeb2"></a>`name` | "WorkflowPlanIntent" |
| <a id="s-c87970994e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkflowPlanIntent.from_plan](stove0-protocol-workflowplanintent-from-plan.md)
- [stove0_protocol.WorkflowPlanIntent.materialize](stove0-protocol-workflowplanintent-materialize.md)
- [stove0_protocol.WorkflowPlanIntent.validate_retirement](stove0-protocol-workflowplanintent-validate-retirement.md)

## Governing policies

- <a id="pa-72e38e0b01"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlanIntent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 382dfcaa8715feac15ce99c5b4231c4bf3b1e3976bd4538215dbe141293d7133 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3ee63b3d0a336d7ca1790729a7ffab36688bc08ad7cc692e025e59701be915b8",
    "signature": "\"(*, operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPlanIntent",
  "unit": "export"
}
```
