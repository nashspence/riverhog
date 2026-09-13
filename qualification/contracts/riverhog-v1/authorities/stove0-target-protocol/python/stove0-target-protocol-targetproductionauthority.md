# stove0_target_protocol.TargetProductionAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetproductionauthority:eb77550338 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3500257307"></a>
| Field | Shape |
|---|---|
| <a id="s-50711ba80f"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-f4cae559bd"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-bbc83daa53"></a>`module` | "stove0_target_protocol" |
| <a id="s-13bec5d0df"></a>`name` | "TargetProductionAuthority" |
| <a id="s-2a882066c4"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetProductionAuthority.seal](stove0-target-protocol-targetproductionauthority-seal.md)
- [stove0_target_protocol.TargetProductionAuthority.verify_digest](stove0-target-protocol-targetproductionauthority-verify-digest.md)

## Governing policies

- <a id="pa-dd055d8e9c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProductionAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a84908b862f589211ab93865dc3168d6ed5da6894b3bba6f4f153ecee250d66 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "955edae0149fdeb8c282f51dd1687bbaab9a525e46b05d22ef13f7fd5a9fc423",
    "signature": "\"(*, format: Literal['stove0-target-production/v1'] = 'stove0-target-production/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], outputs: stove0_target_protocol.protocol.OutputArtifactSetIdentity, disposition_count: Annotated[int, Ge(ge=1)], disposition_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_edge_count: Annotated[int, Ge(ge=1)], source_edge_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], riverhog_disposition_set: riverhog_protocol.collection_workflows.ArtifactDispositionSetIdentity, production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetProductionAuthority",
  "unit": "export"
}
```
