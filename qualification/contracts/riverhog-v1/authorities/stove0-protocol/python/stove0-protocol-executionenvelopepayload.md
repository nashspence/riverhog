# stove0_protocol.ExecutionEnvelopePayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-executionenvelopepayload:0215c4d5d4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc1e484385"></a>
| Field | Shape |
|---|---|
| <a id="s-a037dafddd"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-701586d587"></a>`distribution` | "stove0-protocol" |
| <a id="s-d6af3672c6"></a>`module` | "stove0_protocol" |
| <a id="s-7700ad1d17"></a>`name` | "ExecutionEnvelopePayload" |
| <a id="s-917fe1f086"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ExecutionEnvelopePayload.canonical_claim_id](stove0-protocol-executionenvelopepayload-canonical-claim-id.md)
- [stove0_protocol.ExecutionEnvelopePayload.bind_target](stove0-protocol-executionenvelopepayload-bind-target.md)

## Governing policies

- <a id="pa-add3ae9004"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ExecutionEnvelopePayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 884d37891188a8c2ebdc216c8c8b2adf7d50a7c44a554bc7bbdfb3acb686b24b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "07fd0c9dee16b72ccd9cb2e451adfdd8515d19c459761a5e09fc57daea06cf92",
    "signature": "\"(*, format: Literal['stove0-execution-envelope/v1'] = 'stove0-execution-envelope/v1', claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], workflow_plan: stove0_protocol.models.WorkflowPlan, target_plan: stove0_protocol.models.TargetPlanBinding) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ExecutionEnvelopePayload",
  "unit": "export"
}
```
