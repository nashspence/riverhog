# stove0_protocol.JoinWorkMemberBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinworkmemberbinding:f2f4b9c73b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b3c9dba8f8"></a>
| Field | Shape |
|---|---|
| <a id="s-ebce65f4ed"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-dab55dfc97"></a>`distribution` | "stove0-protocol" |
| <a id="s-da35b8f414"></a>`module` | "stove0_protocol" |
| <a id="s-2392f4a239"></a>`name` | "JoinWorkMemberBinding" |
| <a id="s-1884ee9af7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-2b67ee6758"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinWorkMemberBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d31a6e7eeb4de7dd6cdc05bc3dcb44d0f1772546fb9a8b34f3cf8adf70840ce6 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "e690f8cf01c0d8d6a830f122677b627aae3106fead58d4c6f8829b28574369e3",
    "signature": "\"(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, artifact_selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinWorkMemberBinding",
  "unit": "export"
}
```
