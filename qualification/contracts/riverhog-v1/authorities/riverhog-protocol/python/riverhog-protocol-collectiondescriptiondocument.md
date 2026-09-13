# riverhog_protocol.CollectionDescriptionDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiondescriptiondocument:e51d74dc86 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9fe6ad86cc"></a>
| Field | Shape |
|---|---|
| <a id="s-2cc2d893cd"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4a9bb82ae0"></a>`distribution` | "riverhog-protocol" |
| <a id="s-d54543eae1"></a>`module` | "riverhog_protocol" |
| <a id="s-4b65f4bd06"></a>`name` | "CollectionDescriptionDocument" |
| <a id="s-0568af72e8"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionDescriptionDocument.to_json_bytes](riverhog-protocol-collectiondescriptiondocument-to-json-bytes.md)
- [riverhog_protocol.CollectionDescriptionDocument.from_json_bytes](riverhog-protocol-collectiondescriptiondocument-from-json-bytes.md)
- [riverhog_protocol.CollectionDescriptionDocument.validate_identity](riverhog-protocol-collectiondescriptiondocument-validate-identity.md)
- [riverhog_protocol.CollectionDescriptionDocument.seal](riverhog-protocol-collectiondescriptiondocument-seal.md)

## Governing policies

- <a id="pa-640d4e8234"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDescriptionDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b2ad4f68880a2b6504e2482184ca5e6f67abe87050698b94cd6bf3f2ea66e5e -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "07c2258fc5b3e5ca9c82e44c8aa3bfa957ef546503f9925272a54639a25c8b47",
    "signature": "\"(*, format: Literal['riverhog-collection-description/v1'] = 'riverhog-collection-description/v1', archive_root_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], revision: Annotated[int, Strict(strict=True), Ge(ge=1), Le(le=9007199254740991)], description: CollectionDescription | None, description_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionDescriptionDocument",
  "unit": "export"
}
```
