# stove0_protocol.JoinDeclaration.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joindeclaration-seal:80d59d8cde -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-24e9421ae7"></a>
| Field | Shape |
|---|---|
| <a id="s-dd97179b44"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e3339b742d"></a>`distribution` | "stove0-protocol" |
| <a id="s-36753b7263"></a>`module` | "stove0_protocol" |
| <a id="s-a0418b88b9"></a>`name` | "seal" |
| <a id="s-a9d38a6a17"></a>`owner` | "stove0_protocol.JoinDeclaration" |
| <a id="s-0f6d0e09f0"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.JoinDeclaration](stove0-protocol-joindeclaration.md)

## Governing policies

- <a id="pa-6e3c2f1118"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinDeclaration.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f28b01f9da570fcbbbe6d0d13258c184f610e0c27c41718c6ac1c56c008c57c0 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, members: 'Sequence[JoinMemberDeclaration]', recipe: 'RecipeRef', effective_intent: 'Mapping[str, JsonValue]', workflow_intent: 'WorkflowPlanIntent') -> 'JoinDeclaration'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.JoinDeclaration",
  "unit": "member"
}
```
