# stove0_protocol.JoinMemberDeclaration.canonical_roles

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinmemberdeclaration-can-b45aa06643:3d214ebad5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c16b2e3075"></a>
| Field | Shape |
|---|---|
| <a id="s-3905dbaf6f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-5d0086f503"></a>`distribution` | "stove0-protocol" |
| <a id="s-67605f033b"></a>`module` | "stove0_protocol" |
| <a id="s-1c1dbd3267"></a>`name` | "canonical_roles" |
| <a id="s-a9a3a2c488"></a>`owner` | "stove0_protocol.JoinMemberDeclaration" |
| <a id="s-b6404694df"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.JoinMemberDeclaration](stove0-protocol-joinmemberdeclaration.md)

## Governing policies

- <a id="pa-ae8b965ba0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinMemberDeclaration.canonical_roles`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 234d7d6dff101920f5eb71c318b69b11f3b291c6e1990d3cc14ac26425f5c769 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_roles",
  "owner": "stove0_protocol.JoinMemberDeclaration",
  "unit": "member"
}
```
