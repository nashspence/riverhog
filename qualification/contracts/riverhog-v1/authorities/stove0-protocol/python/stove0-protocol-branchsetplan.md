# stove0_protocol.BranchSetPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetplan:a61ab9e1a7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0c9b65240a"></a>
| Field | Shape |
|---|---|
| <a id="s-9d8e775435"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-bf0eeb1c6c"></a>`distribution` | "stove0-protocol" |
| <a id="s-2be05d98be"></a>`module` | "stove0_protocol" |
| <a id="s-35a648cc6b"></a>`name` | "BranchSetPlan" |
| <a id="s-ca6c6f17aa"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchSetPlan.canonical_branches](stove0-protocol-branchsetplan-canonical-branches.md)
- [stove0_protocol.BranchSetPlan.canonical_bytes](stove0-protocol-branchsetplan-canonical-bytes.md)
- [stove0_protocol.BranchSetPlan.canonical_evidence](stove0-protocol-branchsetplan-canonical-evidence.md)
- [stove0_protocol.BranchSetPlan.seal](stove0-protocol-branchsetplan-seal.md)
- [stove0_protocol.BranchSetPlan.verify_contract](stove0-protocol-branchsetplan-verify-contract.md)

## Governing policies

- <a id="pa-72bff8eb17"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d190b3ea1c3d9ede79ad86044aa8534cfcc6b470b318f3d2e7aa1984e479e51a -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "11539570d1e9fdacda5e7aab1672281a8835e6d6b717b0faa07d8da1aabc0294",
    "signature": "\"(*, format: Literal['stove0-branch-set/v1'] = 'stove0-branch-set/v1', parent_work: stove0_protocol.models.WorkIdentity, decision_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], evidence_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = (), branches: Annotated[tuple[Annotated[stove0_protocol.fork_join.BranchPlan | stove0_protocol.fork_join.CoordinationBranchPlan, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], join: stove0_protocol.fork_join.JoinDeclaration | None = None, retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchSetPlan",
  "unit": "export"
}
```
