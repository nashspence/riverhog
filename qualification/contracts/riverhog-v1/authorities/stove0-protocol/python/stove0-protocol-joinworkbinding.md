# stove0_protocol.JoinWorkBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinworkbinding:c40b7c7cd6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b854206772"></a>
| Field | Shape |
|---|---|
| <a id="s-07ece426bd"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0691d60f59"></a>`distribution` | "stove0-protocol" |
| <a id="s-a59507a0fd"></a>`module` | "stove0_protocol" |
| <a id="s-df624b9773"></a>`name` | "JoinWorkBinding" |
| <a id="s-38db47d563"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.JoinWorkBinding.canonical_members](stove0-protocol-joinworkbinding-canonical-members.md)

## Governing policies

- <a id="pa-69987bcfd6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinWorkBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b156c4c853657e03ec965f6dce35ea7e05c2cd7c6150637a2da46237d33773bc -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "9466a9060b07855b7c74730ec2a1866e96a403f35ff43ce1352e6a4eb80eeafd",
    "signature": "\"(*, kind: Literal['join'] = 'join', parent_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], members: Annotated[tuple[stove0_protocol.models.JoinWorkMemberBinding, ...], MinLen(min_length=2)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinWorkBinding",
  "unit": "export"
}
```
