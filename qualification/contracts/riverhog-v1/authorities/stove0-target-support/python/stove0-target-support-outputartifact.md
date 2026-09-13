# stove0_target_support.OutputArtifact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-outputartifact:befdf946a0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f1b276e508"></a>
| Field | Shape |
|---|---|
| <a id="s-7e83f9ee4c"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-39e7eb9d24"></a>`distribution` | "stove0-target-support" |
| <a id="s-57cabfc967"></a>`module` | "stove0_target_support" |
| <a id="s-a24d9c077e"></a>`name` | "OutputArtifact" |
| <a id="s-68c2a94333"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.OutputArtifact.canonical_path](stove0-target-support-outputartifact-canonical-path.md)

## Governing policies

- <a id="pa-0fd3e29a66"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.OutputArtifact`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42112e1f0d6fe184237b5b2c97df0700167c9d9cf238b0a377ce3d664e56c45b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "199abbe3d7d9a3ea586f4c18283a58e117b7bb43bdd4897f373220d08ee83dc1",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "OutputArtifact",
  "unit": "export"
}
```
