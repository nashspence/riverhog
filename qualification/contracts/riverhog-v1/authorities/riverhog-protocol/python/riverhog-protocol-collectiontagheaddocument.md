# riverhog_protocol.CollectionTagHeadDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagheaddocument:2f399c40f1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d87acca02f"></a>
| Field | Shape |
|---|---|
| <a id="s-741874542b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2f08781b6f"></a>`distribution` | "riverhog-protocol" |
| <a id="s-34e21bc7ea"></a>`module` | "riverhog_protocol" |
| <a id="s-54b5e7f799"></a>`name` | "CollectionTagHeadDocument" |
| <a id="s-2318c138cc"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionTagHeadDocument.validate_identities](riverhog-protocol-collectiontagheaddocument-validate-identities.md)
- [riverhog_protocol.CollectionTagHeadDocument.to_json_bytes](riverhog-protocol-collectiontagheaddocument-to-json-bytes.md)
- [riverhog_protocol.CollectionTagHeadDocument.from_json_bytes](riverhog-protocol-collectiontagheaddocument-from-json-bytes.md)
- [riverhog_protocol.CollectionTagHeadDocument.seal](riverhog-protocol-collectiontagheaddocument-seal.md)

## Governing policies

- <a id="pa-5061c62fe1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagHeadDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c4a75cc082ec10f6337ce4f07937d6be38193906fd3ac6865cc8144cab8298fb -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5bfbd73e084b27df7a341016b42ceb6b1e832cd53927e2f27a1d983a1e927bbe",
    "signature": "\"(*, format: Literal['riverhog-collection-tag-head/v1'] = 'riverhog-collection-tag-head/v1', archive_root_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], revision: Annotated[int, Strict(strict=True), Ge(ge=1), Le(le=9007199254740991)], root_sha256: Annotated[str | None, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')] = None, tag_set_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], head_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionTagHeadDocument",
  "unit": "export"
}
```
