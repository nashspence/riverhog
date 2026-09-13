# riverhog_protocol.CollectionUploadUnitSourceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitsourcedocument:ec920a5b74 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5a17be18e2"></a>
| Field | Shape |
|---|---|
| <a id="s-7e68d8b98c"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4be13710e8"></a>`distribution` | "riverhog-protocol" |
| <a id="s-760abd5d8b"></a>`module` | "riverhog_protocol" |
| <a id="s-86bb11ff9f"></a>`name` | "CollectionUploadUnitSourceDocument" |
| <a id="s-32196e6fd1"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadUnitSourceDocument.canonical_path](riverhog-protocol-collectionuploadunitsourcedocument-canonical-path.md)

## Governing policies

- <a id="pa-5f6e95d62c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitSourceDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f329c7313d5faeb3ab251e757a81393f46d52be73a6dd726555060764d31b67b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3870870a9e0b96323d9a6bb59e3124806d4f2cbfd6db047e34765004a7c96160",
    "signature": "\"(*, path: str, offset: Annotated[int, Strict(strict=True), Ge(ge=0)], bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], artifact_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadUnitSourceDocument",
  "unit": "export"
}
```
