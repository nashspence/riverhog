# riverhog_protocol.CollectionDerivationResponseDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivationres-d949202780:89f510ae30 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c3d08fda3a"></a>
| Field | Shape |
|---|---|
| <a id="s-0736ef4662"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ac435e56c4"></a>`distribution` | "riverhog-protocol" |
| <a id="s-cca0879585"></a>`module` | "riverhog_protocol" |
| <a id="s-9d3487bfae"></a>`name` | "CollectionDerivationResponseDocument" |
| <a id="s-f80e25846f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionDerivationResponseDocument.validate_identity](riverhog-protocol-collectionderivationresponsedocument-validate-identity.md)

## Governing policies

- <a id="pa-413f4ca3f0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivationResponseDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e6860e2feaf178b818bc025b01237ad5ba43b58b4158012f6011aa6b4b336c4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "9c695c7ea7b9395f3726186d8583ad4c36da7d5e751eb7eef54a173fd25df4d3",
    "signature": "\"(*, collection_id: CollectionId, document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], derivation: riverhog_protocol.collection_workflow_transport.CollectionDerivationDocument) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionDerivationResponseDocument",
  "unit": "export"
}
```
