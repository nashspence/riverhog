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
- <a id="s-34a91cb1db"></a>`distribution`: `riverhog-protocol`
- <a id="s-bbe94b5bbe"></a>`module`: `riverhog_protocol`
- <a id="s-4f92d564d3"></a>`name`: `ProcessingOutcomeIdentityDocument`
- <a id="s-141ffa151f"></a>`unit`: `export`

### Declared structure

- <a id="s-7ed6cc02e9"></a>`kind`: `"class"`
- <a id="s-ff8b884dc9"></a>`signature`: `"\"(*, outcome_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], source_claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], output_collection: riverhog_protocol.collection_workflow_transport.CollectionRootIdentityDocument, derivation_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-0f42bb2ef8"></a>
- <a id="s-7f84618032"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7888da1110"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-11ae19fbde"></a>`outcome_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a92119db8c"></a>`output_collection` | yes | #/$defs/CollectionRootIdentityDocument |  |
| <a id="s-680392bdca"></a>`source_claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-3475778270"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-7aa06b3d4f"></a>`CollectionRootIdentityDocument` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [get](riverhog-protocol-processingoutcomeidentitydocument-get.md)
- [__getitem__](riverhog-protocol-processingoutcomeidentitydocument-getitem.md)
- [validate_identity](riverhog-protocol-processingoutcomeidentitydocument-validate-identity.md)

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

<!-- exact-contract-value: 2a3829064aaeee18225864d4caad9aa114641223634a72ecc20fa14c954bccbd -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "CollectionRootIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "derivation_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "outcome_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "output_collection": {
          "$ref": "#/$defs/CollectionRootIdentityDocument"
        },
        "source_claim_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "outcome_id",
        "source_claim_id",
        "output_collection",
        "derivation_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, outcome_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], source_claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], output_collection: riverhog_protocol.collection_workflow_transport.CollectionRootIdentityDocument, derivation_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingOutcomeIdentityDocument",
  "unit": "export"
}
```
