# riverhog_protocol.CollectionUploadRawDigestBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadrawdige-f88d40f36d:78a1cd9117 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55069b7ab0"></a>
| Field | Shape |
|---|---|
| <a id="s-6adc0d5402"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-a88c1544b6"></a>`distribution` | "riverhog-protocol" |
| <a id="s-4ed6434195"></a>`module` | "riverhog_protocol" |
| <a id="s-85798758e2"></a>`name` | "CollectionUploadRawDigestBatchDocument" |
| <a id="s-d6e05578ec"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadRawDigestBatchDocument.canonical_path](riverhog-protocol-collectionuploadrawdigestbatchdocument-canonical-path.md)

## Governing policies

- <a id="pa-6c7285cf7f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRawDigestBatchDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c9bc98c3437bda01ba192cc838ae94bc101ca77695b59dae1ec84252daf7541 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "b3e9c783bf5afec4ecfc01d05a15564e73cead5a37ad60e657d59c0a4e586f01",
    "signature": "\"(*, path: str, first_part: Annotated[int, Strict(strict=True), Ge(ge=0)], sha256s: Annotated[list[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]], MinLen(min_length=1), MaxLen(max_length=1024)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadRawDigestBatchDocument",
  "unit": "export"
}
```
