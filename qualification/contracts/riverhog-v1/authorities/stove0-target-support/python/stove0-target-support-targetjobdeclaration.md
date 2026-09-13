# stove0_target_support.TargetJobDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobdeclaration:2537fcd64f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a994460853"></a>
| Field | Shape |
|---|---|
| <a id="s-fa91864ed9"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-bc0b7ab172"></a>`distribution` | "stove0-target-support" |
| <a id="s-19f1ca9bdd"></a>`module` | "stove0_target_support" |
| <a id="s-48e4a253d5"></a>`name` | "TargetJobDeclaration" |
| <a id="s-269780d130"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetJobDeclaration.bind_execution](stove0-target-support-targetjobdeclaration-bind-execution.md)
- [stove0_target_support.TargetJobDeclaration.canonical_claim_id](stove0-target-support-targetjobdeclaration-canonical-claim-id.md)

## Governing policies

- <a id="pa-43f6883e96"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobDeclaration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb43e89289bc4234e2b55babf35375791b6591b36d0536a07cb170b14f649105 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a0db588dffc46f0fcadc83e704edb92879ec0aa3f006d9c2d369f36c76e0f4a9",
    "signature": "\"(*, job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], controller_evidence: stove0_protocol.models.ControllerEvidence, plan: stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan, workspace_assurance: Literal['encrypted', 'ephemeral']) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetJobDeclaration",
  "unit": "export"
}
```
