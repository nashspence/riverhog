# stove0_target_protocol.OutputArtifactRoleCount

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputartifactrolecount:bb748c2d53 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-015e7541dc"></a>
| Field | Shape |
|---|---|
| <a id="s-4785cc3ae2"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6e962bab8e"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-bde57fdf15"></a>`module` | "stove0_target_protocol" |
| <a id="s-fbb8f47fe2"></a>`name` | "OutputArtifactRoleCount" |
| <a id="s-faf0b1c5a4"></a>`unit` | "export" |

## Governing policies

- <a id="pa-37de7cf356"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputArtifactRoleCount`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f299956d0ae1707933fd33d087687baec74cc539ac9423358354f49b645c36c3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "932e988623cd6631a9f8d0b64a0098cab1ac4a1f70d8609fa8b89676f3c82caf",
    "signature": "\"(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], count: Annotated[int, Ge(ge=1)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "OutputArtifactRoleCount",
  "unit": "export"
}
```
