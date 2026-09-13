# stove0_protocol.ExecutionEnvelope

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-executionenvelope:b14e4c5ac5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98796758ff"></a>
| Field | Shape |
|---|---|
| <a id="s-a9049b92e5"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-11312b4acc"></a>`distribution` | "stove0-protocol" |
| <a id="s-065a90bc8c"></a>`module` | "stove0_protocol" |
| <a id="s-1df444e898"></a>`name` | "ExecutionEnvelope" |
| <a id="s-a99d2e2f02"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ExecutionEnvelope.seal](stove0-protocol-executionenvelope-seal.md)
- [stove0_protocol.ExecutionEnvelope.verify_digest](stove0-protocol-executionenvelope-verify-digest.md)

## Governing policies

- <a id="pa-b6b3381d3f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ExecutionEnvelope`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a86ef9b73a9b342428fc3a1365f39e6d3bf42f7eb92f92cf187196e9f30bea16 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5f6955d3291863e0bdb8bc5e043504aa895c10bca411253c174cf22da6af370f",
    "signature": "\"(*, format: Literal['stove0-execution-envelope/v1'] = 'stove0-execution-envelope/v1', claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], workflow_plan: stove0_protocol.models.WorkflowPlan, target_plan: stove0_protocol.models.TargetPlanBinding, execution_envelope_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ExecutionEnvelope",
  "unit": "export"
}
```
