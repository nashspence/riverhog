# stove0_protocol.ArtifactSubject

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactsubject:a3d3043c40 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-32aa2841b1"></a>
| Field | Shape |
|---|---|
| <a id="s-61017d9db0"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-1eca95374d"></a>`distribution` | "stove0-protocol" |
| <a id="s-5d0f4b28fe"></a>`module` | "stove0_protocol" |
| <a id="s-38e0c022bf"></a>`name` | "ArtifactSubject" |
| <a id="s-e8f1de62c7"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ArtifactSubject.canonical_path](stove0-protocol-artifactsubject-canonical-path.md)

## Governing policies

- <a id="pa-c9b064112f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSubject`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad5ef85651236d946861427bb4446688d4e14d67888fc248618e80df60baee3b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "e232a8cd82feef9c0182803e046cc2d761c6cc9a66de08fcb1aa58eb275a86c6",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], collection: stove0_protocol.models.CollectionRootRef, path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ArtifactSubject",
  "unit": "export"
}
```
