# stove0_target_protocol.OutputArtifact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputartifact:57db3aaec8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-226e0712a1"></a>
| Field | Shape |
|---|---|
| <a id="s-44e31c3e09"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-960361c334"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-600e328fb6"></a>`module` | "stove0_target_protocol" |
| <a id="s-caddb1aa4d"></a>`name` | "OutputArtifact" |
| <a id="s-1c0db880d7"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OutputArtifact.canonical_path](stove0-target-protocol-outputartifact-canonical-path.md)

## Governing policies

- <a id="pa-8d24254c33"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputArtifact`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e1e7425fe7017ff4308fb7656ec97e61e6595b70a27e176111d774443c9ab04e -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "199abbe3d7d9a3ea586f4c18283a58e117b7bb43bdd4897f373220d08ee83dc1",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "OutputArtifact",
  "unit": "export"
}
```
