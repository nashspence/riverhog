# riverhog_protocol.ProcessingOutcomeIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingoutcomeidentitydocument:47b9e7fec1 -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-7f84618032"></a>`type`: `"object"`
- <a id="s-024f86a7e3"></a>`additionalProperties`: `false`
- <a id="s-5ea768c7e0"></a>`required`: `["outcome_id","source_claim_id","output_collection","derivation_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7888da1110"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-11ae19fbde"></a>`outcome_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a92119db8c"></a>`output_collection` | yes | [CollectionRootIdentityDocument](#s-7aa06b3d4f) |  |
| <a id="s-680392bdca"></a>`source_claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionId](#s-3475778270)
- [CollectionRootIdentityDocument](#s-7aa06b3d4f)

##### <a id="s-3475778270"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-e7c3142235"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-8f7b524f29"></a>2 | not=(const="0") |

##### <a id="s-7aa06b3d4f"></a>definition `CollectionRootIdentityDocument`

- <a id="s-2f9ef730e3"></a>`type`: `"object"`
- <a id="s-5ecf0f050e"></a>`additionalProperties`: `false`
- <a id="s-b225dfb67d"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7bcd71f12c"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b32d7f5055"></a>`collection_id` | yes | [CollectionId](#s-3475778270) |  |
| <a id="s-b80ed7c06d"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [get](riverhog-protocol-processingoutcomeidentitydocument-get.md)
- [__getitem__](riverhog-protocol-processingoutcomeidentitydocument-getitem.md)
- [validate_identity](riverhog-protocol-processingoutcomeidentitydocument-validate-identity.md)

## Governing policies

- <a id="pa-bf1781b174"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingOutcomeIdentityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9eb493235c8c77d4d5e8e4734705a9b3fd227fcee326f3137c97029cac46ea90 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "allOf": [
            {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            },
            {
              "not": {
                "const": "0"
              }
            }
          ]
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

</details>
