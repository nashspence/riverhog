# stove0_protocol.WorkflowPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplan:1051bd4de8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-16449ced4b"></a>
| Field | Shape |
|---|---|
| <a id="s-3cde83f243"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ce3685dc1d"></a>`distribution` | "stove0-protocol" |
| <a id="s-db14ca4d38"></a>`module` | "stove0_protocol" |
| <a id="s-aa36e507f9"></a>`name` | "WorkflowPlan" |
| <a id="s-4de37a1b2e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkflowPlan.seal](stove0-protocol-workflowplan-seal.md)
- [stove0_protocol.WorkflowPlan.verify_digest](stove0-protocol-workflowplan-verify-digest.md)

## Governing policies

- <a id="pa-35e57ffe6b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1fc09e56656c59ccd9f0b35d24f7935adf188030c20adf523ec76f5fcb9aecdf -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "dc8f2cc9d162e6b40e2d3abe594494d91c2fa0d071396788b9a0b7df86f64088",
    "signature": "\"(*, format: Literal['stove0-workflow-plan/v1'] = 'stove0-workflow-plan/v1', work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>, workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPlan",
  "unit": "export"
}
```
