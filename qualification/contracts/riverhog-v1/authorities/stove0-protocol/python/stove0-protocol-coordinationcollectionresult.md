# stove0_protocol.CoordinationCollectionResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationcollectionresult:f9cfb844c5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fa965f5bc0"></a>
| Field | Shape |
|---|---|
| <a id="s-c8b53bb0d5"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ed71943913"></a>`distribution` | "stove0-protocol" |
| <a id="s-98d8031b1c"></a>`module` | "stove0_protocol" |
| <a id="s-bfc2a73cdc"></a>`name` | "CoordinationCollectionResult" |
| <a id="s-9465c59ef7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-93d882f42f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationCollectionResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 38bcab70ee5c422d4730aca848d9cd745ef73c23f7d3ef199b8991409fb7edb4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5fd6c013e6da69ffca712f53a43fde736783250dd5dda7a0a176a2a9f4eeb5bf",
    "signature": "\"(*, producer_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "CoordinationCollectionResult",
  "unit": "export"
}
```
