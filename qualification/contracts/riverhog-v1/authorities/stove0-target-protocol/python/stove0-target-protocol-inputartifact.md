# stove0_target_protocol.InputArtifact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-inputartifact:9b4c772585 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f68b926b5c"></a>
| Field | Shape |
|---|---|
| <a id="s-f50e9b0778"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-8d5b39423a"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-28eb2d0fa0"></a>`module` | "stove0_target_protocol" |
| <a id="s-41534db0a5"></a>`name` | "InputArtifact" |
| <a id="s-eb22828608"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.InputArtifact.canonical_path](stove0-target-protocol-inputartifact-canonical-path.md)

## Governing policies

- <a id="pa-86656096e0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.InputArtifact`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74afb2cc63452af445176e7c3b36845a0a53329a29c1d87920a60b4960fd44ec -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6bae5bc6fdacfc380db9fd5107b64cf18ed580f1888a45a1b1a6e37731e60168",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], collection: stove0_protocol.models.CollectionRootRef, path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "InputArtifact",
  "unit": "export"
}
```
