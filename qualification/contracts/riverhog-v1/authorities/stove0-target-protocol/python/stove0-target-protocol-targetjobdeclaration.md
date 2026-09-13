# stove0_target_protocol.TargetJobDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobdeclaration:6890bad112 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-840aaf96df"></a>
| Field | Shape |
|---|---|
| <a id="s-193f265af1"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-559b9a4f2e"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-a3f2bcebc6"></a>`module` | "stove0_target_protocol" |
| <a id="s-327c1827bc"></a>`name` | "TargetJobDeclaration" |
| <a id="s-eef2dc2d76"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetJobDeclaration.canonical_claim_id](stove0-target-protocol-targetjobdeclaration-canonical-claim-id.md)
- [stove0_target_protocol.TargetJobDeclaration.bind_execution](stove0-target-protocol-targetjobdeclaration-bind-execution.md)

## Governing policies

- <a id="pa-314e6aa61f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobDeclaration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b1f2562afbdda0aa0cf481e86bb21d9952ca34d7387aaf1b9c71cdff88be4d1 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a0db588dffc46f0fcadc83e704edb92879ec0aa3f006d9c2d369f36c76e0f4a9",
    "signature": "\"(*, job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], controller_evidence: stove0_protocol.models.ControllerEvidence, plan: stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan, workspace_assurance: Literal['encrypted', 'ephemeral']) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetJobDeclaration",
  "unit": "export"
}
```
