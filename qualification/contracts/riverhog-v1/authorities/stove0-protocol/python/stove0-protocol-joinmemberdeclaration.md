# stove0_protocol.JoinMemberDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinmemberdeclaration:e58adb3faa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2ff4deeda4"></a>
| Field | Shape |
|---|---|
| <a id="s-08fd32b897"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-bd0e79df53"></a>`distribution` | "stove0-protocol" |
| <a id="s-54e93e7d58"></a>`module` | "stove0_protocol" |
| <a id="s-ef4b90cf8d"></a>`name` | "JoinMemberDeclaration" |
| <a id="s-80443e9007"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.JoinMemberDeclaration.canonical_roles](stove0-protocol-joinmemberdeclaration-canonical-roles.md)

## Governing policies

- <a id="pa-dbb3996109"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinMemberDeclaration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97f4739c4077a05ba465d066f3956cb109ec2b38005d55a855435bf0103a16c0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "f1b1441297c6334a7eeb79e0a05030fe3b536cc2d6ee09a2aa316a623e74c389",
    "signature": "\"(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], output_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinMemberDeclaration",
  "unit": "export"
}
```
