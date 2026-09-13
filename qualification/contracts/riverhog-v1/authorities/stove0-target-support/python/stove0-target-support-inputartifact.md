# stove0_target_support.InputArtifact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-inputartifact:5e3f963f5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d3a77d787"></a>
| Field | Shape |
|---|---|
| <a id="s-6d1d6689eb"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-8d7acc02ee"></a>`distribution` | "stove0-target-support" |
| <a id="s-fd683a4f72"></a>`module` | "stove0_target_support" |
| <a id="s-7e1236e72f"></a>`name` | "InputArtifact" |
| <a id="s-6593f2a2d3"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.InputArtifact.canonical_path](stove0-target-support-inputartifact-canonical-path.md)

## Governing policies

- <a id="pa-0011521cba"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.InputArtifact`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b135bfc51972bc1ed12e2f979c02e1211d4f70bf73747faf924568002df1ff5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6bae5bc6fdacfc380db9fd5107b64cf18ed580f1888a45a1b1a6e37731e60168",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], collection: stove0_protocol.models.CollectionRootRef, path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "InputArtifact",
  "unit": "export"
}
```
