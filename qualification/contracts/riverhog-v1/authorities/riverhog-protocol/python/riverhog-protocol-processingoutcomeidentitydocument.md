# riverhog_protocol.ProcessingOutcomeIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingoutcomeidentitydocument:47b9e7fec1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-716c48f27f"></a>
| Field | Shape |
|---|---|
| <a id="s-3f68b0efd9"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-34a91cb1db"></a>`distribution` | "riverhog-protocol" |
| <a id="s-bbe94b5bbe"></a>`module` | "riverhog_protocol" |
| <a id="s-4f92d564d3"></a>`name` | "ProcessingOutcomeIdentityDocument" |
| <a id="s-141ffa151f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ProcessingOutcomeIdentityDocument.validate_identity](riverhog-protocol-processingoutcomeidentitydocument-validate-identity.md)

## Governing policies

- <a id="pa-bf1781b174"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingOutcomeIdentityDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0af52942172d487a50401982b6ee1753e390e84852204182b90f1fc081963a2b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "8dc6d3031e0dc17cc17e6e2c78c2f72b79a129fcc79822a1b2dd39c77972651f",
    "signature": "\"(*, outcome_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], source_claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], output_collection: riverhog_protocol.collection_workflow_transport.CollectionRootIdentityDocument, derivation_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingOutcomeIdentityDocument",
  "unit": "export"
}
```
