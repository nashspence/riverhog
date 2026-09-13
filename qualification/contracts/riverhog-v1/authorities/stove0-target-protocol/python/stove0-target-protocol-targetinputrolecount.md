# stove0_target_protocol.TargetInputRoleCount

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetinputrolecount:53c6e502a1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0e17e633bc"></a>
| Field | Shape |
|---|---|
| <a id="s-e0815a4dac"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-520fd5df42"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-f81c03923d"></a>`module` | "stove0_target_protocol" |
| <a id="s-55e4798e8e"></a>`name` | "TargetInputRoleCount" |
| <a id="s-87bf0cfaf0"></a>`unit` | "export" |

## Governing policies

- <a id="pa-99b988e9fa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetInputRoleCount`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f608b8b02fdd43aae72e9e075b02076d42ed71672bbf7fc1867e07e67991dac3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "cd93b356fb498257b255f7262952a01f1fa657555e9644325bdee9e3c4f14028",
    "signature": "\"(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], count: Annotated[int, Ge(ge=1)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetInputRoleCount",
  "unit": "export"
}
```
