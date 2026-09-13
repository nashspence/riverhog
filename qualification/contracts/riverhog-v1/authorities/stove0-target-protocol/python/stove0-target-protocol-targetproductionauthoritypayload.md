# stove0_target_protocol.TargetProductionAuthorityPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetproductionau-bd5eaa29a7:a3fb481714 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7fe32289dc"></a>
| Field | Shape |
|---|---|
| <a id="s-45d5b98028"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-24148d2bed"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-2ab5d8d881"></a>`module` | "stove0_target_protocol" |
| <a id="s-89cd56c52c"></a>`name` | "TargetProductionAuthorityPayload" |
| <a id="s-f860e544d6"></a>`unit` | "export" |

## Governing policies

- <a id="pa-eaa6d9a059"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProductionAuthorityPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aac7ec6d9d5357ec027ec8edbba998f710da7d17d2554b5eae0e860b44299b93 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "33f746fc9f49278dbbf1462ee186ee0e11fe95c8830e1a6d406631252d405022",
    "signature": "\"(*, format: Literal['stove0-target-production/v1'] = 'stove0-target-production/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], outputs: stove0_target_protocol.protocol.OutputArtifactSetIdentity, disposition_count: Annotated[int, Ge(ge=1)], disposition_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_edge_count: Annotated[int, Ge(ge=1)], source_edge_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], riverhog_disposition_set: riverhog_protocol.collection_workflows.ArtifactDispositionSetIdentity) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetProductionAuthorityPayload",
  "unit": "export"
}
```
