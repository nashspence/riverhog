# stove0_protocol.JoinDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joindeclaration:3bfa77fe24 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1b4ba953c4"></a>
| Field | Shape |
|---|---|
| <a id="s-62a2dd238d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-def1a1128f"></a>`distribution` | "stove0-protocol" |
| <a id="s-61a837bb45"></a>`module` | "stove0_protocol" |
| <a id="s-91afe9b9c8"></a>`name` | "JoinDeclaration" |
| <a id="s-296c5c4a48"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.JoinDeclaration.canonical_members](stove0-protocol-joindeclaration-canonical-members.md)
- [stove0_protocol.JoinDeclaration.seal](stove0-protocol-joindeclaration-seal.md)
- [stove0_protocol.JoinDeclaration.verify_contract](stove0-protocol-joindeclaration-verify-contract.md)

## Governing policies

- <a id="pa-3288ab02f2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinDeclaration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 470a279561a13e7785cf7953b00976de1443f8f006b562b46d0715674273374c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "59b7a82cb0367a896054d30d8696a7585892d491b7e53c80ef5205434d902f06",
    "signature": "\"(*, format: Literal['stove0-join-declaration/v1'] = 'stove0-join-declaration/v1', members: Annotated[tuple[stove0_protocol.fork_join.JoinMemberDeclaration, ...], MinLen(min_length=2)], recipe: stove0_protocol.models.RecipeRef, effective_intent: dict[str, JsonValue] = <factory>, workflow_intent: stove0_protocol.models.WorkflowPlanIntent, join_declaration_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinDeclaration",
  "unit": "export"
}
```
