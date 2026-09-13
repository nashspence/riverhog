# riverhog_protocol.CollectionRootIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionrootidentitydocument:2894e47535 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b9c1e1d8ca"></a>
| Field | Shape |
|---|---|
| <a id="s-909bb36ffd"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-a8298c29cb"></a>`distribution` | "riverhog-protocol" |
| <a id="s-0ab7238084"></a>`module` | "riverhog_protocol" |
| <a id="s-bb2a873c77"></a>`name` | "CollectionRootIdentityDocument" |
| <a id="s-6bb54a8bdf"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionRootIdentityDocument.validate_identity](riverhog-protocol-collectionrootidentitydocument-validate-identity.md)

## Governing policies

- <a id="pa-e8fe5095eb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionRootIdentityDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7cec6eb3cddb61bbd4771ff82c16e9541bc16700267cac227ba798d5eb4e8cc -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c0af18baab5624495242a4ccde4d6cef1e59c267f9902b78bc5182edeb970f35",
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], content_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionRootIdentityDocument",
  "unit": "export"
}
```
