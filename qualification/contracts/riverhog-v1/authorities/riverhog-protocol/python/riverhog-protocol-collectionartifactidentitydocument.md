# riverhog_protocol.CollectionArtifactIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionartifactidentitydocument:035869f261 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-956255dfcd"></a>
| Field | Shape |
|---|---|
| <a id="s-28cd003c50"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-3d98dc5a07"></a>`distribution` | "riverhog-protocol" |
| <a id="s-76bba8821f"></a>`module` | "riverhog_protocol" |
| <a id="s-d8b8ba46c8"></a>`name` | "CollectionArtifactIdentityDocument" |
| <a id="s-7b1aba30ae"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionArtifactIdentityDocument.validate_identity](riverhog-protocol-collectionartifactidentitydocument-validate-identity.md)

## Governing policies

- <a id="pa-faff1b87a8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionArtifactIdentityDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: feb85f57001648f0134e6dbbfa0447a178f13056e76499e4f6921181ffb8f00d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "149456f1731c2018b68844dd93513f835937fda10ac2bd2fab237237d6f09147",
    "signature": "\"(*, collection: riverhog_protocol.collection_workflow_transport.CollectionRootIdentityDocument, path: CanonicalRelPath, bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionArtifactIdentityDocument",
  "unit": "export"
}
```
