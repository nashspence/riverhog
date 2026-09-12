# riverhog_provenance_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts:6b7a434d1b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-70ad9a8787) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-086605c883"></a>
| Field | Shape |
|---|---|
| <a id="s-514882f135"></a>`distribution` | "riverhog-provenance-contracts" |
| <a id="s-9c1a297173"></a>`exports` | additional keys=`CANONICAL_UUID_URN_PATTERN`, `PROVENANCE_CONTRACT_BINDING_FORMAT`, `PROVENANCE_CONTRACT_ENTRY_POINT_GROUP`, `PROVENANCE_CONTRACT_REFERENCE_FORMAT`, `PROVENANCE_SCHEMA_DIALECT`, `PROVENANCE_SCHEMA_FORMAT_POLICY`, `ProvenanceContractBinding`, `ProvenanceEntryId`, `ProvenanceJournalId`, `ProvenanceJournalStateReference`, `ProvenanceStateId`, `SHA256_PATTERN`, `index_schema_documents`, `require_canonical_uuid_urn` |
| <a id="s-033f7e67b1"></a>`module` | "riverhog_provenance_contracts" |

## Governing policies

- <a id="pa-38c65189cb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts](../../../evidence/sources.md#src-7722019950) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py::<module>`

### Machine authority

- `/external_contract/python/9`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d6c1d56d977683bb678279b9bb0be21cc7f7fa86a1a3614dc5c5d165b334819 -->

```json
{
  "distribution": "riverhog-provenance-contracts",
  "exports": {
    "CANONICAL_UUID_URN_PATTERN": {
      "kind": "constant",
      "value": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    },
    "PROVENANCE_CONTRACT_BINDING_FORMAT": {
      "kind": "constant",
      "value": "riverhog-provenance-contract-binding/v1"
    },
    "PROVENANCE_CONTRACT_ENTRY_POINT_GROUP": {
      "kind": "constant",
      "value": "riverhog.provenance-contracts"
    },
    "PROVENANCE_CONTRACT_REFERENCE_FORMAT": {
      "kind": "constant",
      "value": "riverhog-provenance-contract-reference/v1"
    },
    "PROVENANCE_SCHEMA_DIALECT": {
      "kind": "constant",
      "value": "https://json-schema.org/draft/2020-12/schema"
    },
    "PROVENANCE_SCHEMA_FORMAT_POLICY": {
      "kind": "constant",
      "value": "annotation-only"
    },
    "ProvenanceContractBinding": {
      "fields": [
        {
          "default": "required",
          "name": "format",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "contract_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "contract_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "schema_dialect",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "format_policy",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "_schemas_json",
          "type": "'bytes'"
        }
      ],
      "kind": "class",
      "members": {
        "reference": {
          "kind": "method",
          "signature": "(self, provider: 'str') -> 'dict[str, str]'"
        },
        "schemas": {
          "kind": "property",
          "signature": "(self) -> 'dict[str, dict[str, Any]]'"
        }
      },
      "signature": "(*, contract_id: 'str', schemas: 'Iterable[Mapping[str, Any]]') -> 'None'"
    },
    "ProvenanceEntryId": {
      "kind": "type-alias",
      "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')]), AfterValidator(func=<function _entry_id>)]"
    },
    "ProvenanceJournalId": {
      "kind": "type-alias",
      "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')]), AfterValidator(func=<function _journal_id>)]"
    },
    "ProvenanceJournalStateReference": {
      "kind": "class",
      "schema_sha256": "7f3d93e0df8f97bd47bb76100caba9e088809d1c50826896a8c184ecfb5acaf7",
      "signature": "(*, journal_id: ProvenanceJournalId, current_state_id: ProvenanceStateId) -> None"
    },
    "ProvenanceStateId": {
      "kind": "type-alias",
      "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')]), AfterValidator(func=<function _state_id>)]"
    },
    "SHA256_PATTERN": {
      "kind": "constant",
      "value": "^[0-9a-f]{64}$"
    },
    "index_schema_documents": {
      "kind": "function",
      "signature": "(documents: 'Iterable[Mapping[str, Any]]', *, owner: 'str') -> 'dict[str, dict[str, Any]]'"
    },
    "require_canonical_uuid_urn": {
      "kind": "function",
      "signature": "(value: 'str', field: 'str' = 'identity') -> 'str'"
    }
  },
  "module": "riverhog_provenance_contracts"
}
```
