# stove0_media_metadata_observer_contracts.MediaFactEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-63d3e8c6a9:c328030851 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc20045c1e"></a>
| Field | Shape |
|---|---|
| <a id="s-0c6cd445a8"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-31c1be4d47"></a>`distribution` | "stove0-media-metadata-observer-contracts" |
| <a id="s-d0f0408a45"></a>`module` | "stove0_media_metadata_observer_contracts" |
| <a id="s-a8d8acdcdc"></a>`name` | "MediaFactEvidence" |
| <a id="s-fcd05f3741"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a4747953d1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MediaFactEvidence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea872dbed7f8225eee0d1f46932abc3cb0158513deb3bf0284f230816b7542a1 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "aaa797b126a9df34f6f9669a9b4a92fdad30c0811310a1ccc3086865f377cc37",
    "signature": "'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], field: Annotated[str, MinLen(min_length=1), MaxLen(max_length=240)]) -> None'"
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "MediaFactEvidence",
  "unit": "export"
}
```
