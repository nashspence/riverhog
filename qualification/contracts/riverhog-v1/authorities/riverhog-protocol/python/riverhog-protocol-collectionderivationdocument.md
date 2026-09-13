# riverhog_protocol.CollectionDerivationDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivationdocument:b293f3461a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f5b2ba4914"></a>
| Field | Shape |
|---|---|
| <a id="s-8d9d2f81ed"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-06afa26551"></a>`distribution` | "riverhog-protocol" |
| <a id="s-c5370bb4e5"></a>`module` | "riverhog_protocol" |
| <a id="s-1643a30ba3"></a>`name` | "CollectionDerivationDocument" |
| <a id="s-f4ec65c7db"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionDerivationDocument.validate_derivation](riverhog-protocol-collectionderivationdocument-validate-derivation.md)

## Governing policies

- <a id="pa-356282d768"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivationDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4a20cd52a62b3232c5ff2a869610a3ecd9a72975f9e5baa8f00a0d502204e54c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5d9520bbc343d2b012d0dc1097af4b519366d62d84c1922f078d4a5395964375",
    "signature": "\"(*, format: Literal['riverhog-collection-derivation/v1'], execution_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], claim: riverhog_protocol.collection_workflow_transport.ClaimFenceDocument, recipe: riverhog_protocol.collection_workflow_transport.RecipeIdentityDocument, operation: riverhog_protocol.collection_workflow_transport.OperationIdentityDocument, input_set_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], artifact_set_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_envelope_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], controller_evidence: dict[str, typing.Any], controller_evidence_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], disposition_set: riverhog_protocol.collection_workflow_transport.ArtifactDispositionSetIdentityDocument) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionDerivationDocument",
  "unit": "export"
}
```
