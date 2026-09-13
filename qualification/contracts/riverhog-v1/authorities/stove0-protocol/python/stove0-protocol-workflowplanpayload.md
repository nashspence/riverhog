# stove0_protocol.WorkflowPlanPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplanpayload:7192923642 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-512741deed"></a>
| Field | Shape |
|---|---|
| <a id="s-d942511b0d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-70e3f912d6"></a>`distribution` | "stove0-protocol" |
| <a id="s-de20089f24"></a>`module` | "stove0_protocol" |
| <a id="s-fc2c30d8f6"></a>`name` | "WorkflowPlanPayload" |
| <a id="s-d31a97a13d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkflowPlanPayload.canonical_observations](stove0-protocol-workflowplanpayload-canonical-observations.md)
- [stove0_protocol.WorkflowPlanPayload.protect_evaluation_sources](stove0-protocol-workflowplanpayload-protect-evaluation-sources.md)

## Governing policies

- <a id="pa-fed3628ef6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlanPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29b5e33d692c3406055f599295980fd7a4faaf11e73e6ca638ef610381159af8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "e13c1c2b2adac8dde0ce9197a2b19bded89f69aa6ec359fd68445674beb8af60",
    "signature": "\"(*, format: Literal['stove0-workflow-plan/v1'] = 'stove0-workflow-plan/v1', work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPlanPayload",
  "unit": "export"
}
```
