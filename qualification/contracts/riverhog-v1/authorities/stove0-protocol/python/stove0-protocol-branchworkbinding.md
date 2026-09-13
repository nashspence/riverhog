# stove0_protocol.BranchWorkBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchworkbinding:f9368f4ff1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-09635065f9"></a>
| Field | Shape |
|---|---|
| <a id="s-d7d9f889c4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-e066f4b9aa"></a>`distribution` | "stove0-protocol" |
| <a id="s-18b2c6e5f9"></a>`module` | "stove0_protocol" |
| <a id="s-7d8fbc2752"></a>`name` | "BranchWorkBinding" |
| <a id="s-3929bec70e"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f059635bb7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchWorkBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e42e279e4221f6b940a3301808551000a1ba4c7f5f888dad93b1396875615b1a -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "0c3297729fbc53bc25d0dcb1bf587a91b7991848f5584c8248180f28b506ecd9",
    "signature": "\"(*, kind: Literal['branch'] = 'branch', parent_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], decision_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], artifact_selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchWorkBinding",
  "unit": "export"
}
```
