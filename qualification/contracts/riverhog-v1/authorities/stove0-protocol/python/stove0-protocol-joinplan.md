# stove0_protocol.JoinPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinplan:43f4428e78 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ccfe9b231e"></a>
| Field | Shape |
|---|---|
| <a id="s-d20b844d16"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-1010761575"></a>`distribution` | "stove0-protocol" |
| <a id="s-9c4531c6aa"></a>`module` | "stove0_protocol" |
| <a id="s-45387df416"></a>`name` | "JoinPlan" |
| <a id="s-5bea08eb82"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.JoinPlan.canonical_inputs](stove0-protocol-joinplan-canonical-inputs.md)
- [stove0_protocol.JoinPlan.seal](stove0-protocol-joinplan-seal.md)
- [stove0_protocol.JoinPlan.verify_contract](stove0-protocol-joinplan-verify-contract.md)

## Governing policies

- <a id="pa-3c113d6aab"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f071097919e048e7c8682f17a43d589a9ad0a957f9b7a7728d099dfcf91eb071 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "8210d3195621df847cfbceceea08fa633e6b88328b7bba90ca04fc6faa47280a",
    "signature": "\"(*, format: Literal['stove0-join-plan/v1'] = 'stove0-join-plan/v1', parent_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], declaration: stove0_protocol.fork_join.JoinDeclaration, inputs: Annotated[tuple[stove0_protocol.fork_join.JoinInputPlan, ...], MinLen(min_length=2)], work: stove0_protocol.models.WorkIdentity, workflow_plan: stove0_protocol.models.WorkflowPlan, join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinPlan",
  "unit": "export"
}
```
