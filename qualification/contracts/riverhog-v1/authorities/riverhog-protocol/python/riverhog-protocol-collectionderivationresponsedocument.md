# riverhog_protocol.CollectionDerivationResponseDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivationres-d949202780:89f510ae30 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c3d08fda3a"></a>
- <a id="s-ac435e56c4"></a>`distribution`: `riverhog-protocol`
- <a id="s-cca0879585"></a>`module`: `riverhog_protocol`
- <a id="s-9d3487bfae"></a>`name`: `CollectionDerivationResponseDocument`
- <a id="s-f80e25846f"></a>`unit`: `export`

### Declared structure

- <a id="s-b98b7a96c7"></a>`kind`: `"class"`
- <a id="s-5e7b8d2be1"></a>`signature`: `"\"(*, collection_id: CollectionId, document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], derivation: riverhog_protocol.collection_workflow_transport.CollectionDerivationDocument) -> None\""`

#### Validated model schema

<a id="s-71da214ae0"></a>

- <a id="s-3a49fd707c"></a>`type`: `"object"`
- <a id="s-3d7eca4bd4"></a>`additionalProperties`: `false`
- <a id="s-be04705911"></a>`required`: `["collection_id","document_sha256","derivation"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5dc08d71ba"></a>`collection_id` | yes | [CollectionId](#s-71747045d5) |  |
| <a id="s-ff2e6d297a"></a>`derivation` | yes | [CollectionDerivationDocument](#s-c39938b744) |  |
| <a id="s-fe8fb1e8ae"></a>`document_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactDispositionSetIdentityDocument](#s-e39f407cdc)
- [ClaimFenceDocument](#s-1bcca920fa)
- [CollectionDerivationDocument](#s-c39938b744)
- [CollectionId](#s-71747045d5)
- [NonnegativeDecimal](#s-793dd95850)
- [OperationIdentityDocument](#s-28d97b8c2e)
- [RecipeIdentityDocument](#s-dd912b8d9e)

##### <a id="s-e39f407cdc"></a>definition `ArtifactDispositionSetIdentityDocument`

- <a id="s-0d3b39d0d2"></a>`type`: `"object"`
- <a id="s-a2baa1a373"></a>`additionalProperties`: `false`
- <a id="s-9384af1178"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c16f79c7a3"></a>`disposition_count` | yes | [NonnegativeDecimal](#s-793dd95850); ge=1 |  |
| <a id="s-57bf7912bd"></a>`output_artifact_count` | yes | [NonnegativeDecimal](#s-793dd95850); ge=1 |  |
| <a id="s-c7b17c0cbc"></a>`output_edge_count` | yes | [NonnegativeDecimal](#s-793dd95850); ge=1 |  |
| <a id="s-e6b9134cd2"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-1bcca920fa"></a>definition `ClaimFenceDocument`

- <a id="s-b15bffdf95"></a>`type`: `"object"`
- <a id="s-f06517c073"></a>`additionalProperties`: `false`
- <a id="s-c2b313a133"></a>`required`: `["id","fence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-13736711e9"></a>`fence` | yes | [NonnegativeDecimal](#s-793dd95850); ge=1 |  |
| <a id="s-3221658763"></a>`id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c39938b744"></a>definition `CollectionDerivationDocument`

- <a id="s-503ec1009e"></a>`type`: `"object"`
- <a id="s-9fb783c2a2"></a>`additionalProperties`: `false`
- <a id="s-11e24a9ff2"></a>`required`: `["format","execution_id","claim","recipe","operation","input_set_sha256","artifact_set_sha256","execution_envelope_sha256","execution_sha256","controller_evidence","controller_evidence_sha256","disposition_set"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-44ea3d7bac"></a>`artifact_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-84c99ba76c"></a>`claim` | yes | [ClaimFenceDocument](#s-1bcca920fa) |  |
| <a id="s-41b0b36b93"></a>`controller_evidence` | yes | type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=16777216; x-riverhog-extent={"policy":"contract_max","reason":"bounded-controller-evidence-envelope"} |  |
| <a id="s-f8ba23bd8b"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bc7d7c4b8c"></a>`disposition_set` | yes | [ArtifactDispositionSetIdentityDocument](#s-e39f407cdc) |  |
| <a id="s-cfa9919577"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-254b8a1686"></a>`execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-09861aea09"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2461ade785"></a>`format` | yes | type="string"; const="riverhog-collection-derivation/v1" |  |
| <a id="s-cace6e657a"></a>`input_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a270796b82"></a>`operation` | yes | [OperationIdentityDocument](#s-28d97b8c2e) |  |
| <a id="s-8cef8f5c41"></a>`recipe` | yes | [RecipeIdentityDocument](#s-dd912b8d9e) |  |

##### <a id="s-71747045d5"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-97a628a4a9"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-fe5871219e"></a>2 | not=(const="0") |

##### <a id="s-793dd95850"></a>definition `NonnegativeDecimal`

- <a id="s-b786c4128a"></a>`type`: `"string"`
- <a id="s-1157e7b81b"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-28d97b8c2e"></a>definition `OperationIdentityDocument`

- <a id="s-d50d5db2f1"></a>`type`: `"object"`
- <a id="s-103bded511"></a>`additionalProperties`: `false`
- <a id="s-f6635e8c7c"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-19c312ebed"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5ab472345d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-dd912b8d9e"></a>definition `RecipeIdentityDocument`

- <a id="s-dc30ebb91b"></a>`type`: `"object"`
- <a id="s-0b5b70a27c"></a>`additionalProperties`: `false`
- <a id="s-e320eeef58"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-925a251bfc"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9f061989f9"></a>`revision` | yes | [NonnegativeDecimal](#s-793dd95850); ge=1 |  |
| <a id="s-24ffacb218"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-collectionderivationresponsedocument-getitem.md)
- [validate_identity](riverhog-protocol-collectionderivationresponsedocument-validate-identity.md)
- [get](riverhog-protocol-collectionderivationresponsedocument-get.md)

## Governing policies

- <a id="pa-413f4ca3f0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivationResponseDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ebbdeeda2df136ede5a776492a952e62d5aa1af92780d7309bbd5c3c65d86ed -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactDispositionSetIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "disposition_count": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            "output_artifact_count": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            "output_edge_count": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "disposition_count",
            "output_edge_count",
            "output_artifact_count",
            "sha256"
          ],
          "type": "object"
        },
        "ClaimFenceDocument": {
          "additionalProperties": false,
          "properties": {
            "fence": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            "id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "fence"
          ],
          "type": "object"
        },
        "CollectionDerivationDocument": {
          "additionalProperties": false,
          "properties": {
            "artifact_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "claim": {
              "$ref": "#/$defs/ClaimFenceDocument"
            },
            "controller_evidence": {
              "additionalProperties": true,
              "type": "object",
              "x-riverhog-encoded-bytes-max": 16777216,
              "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-controller-evidence-envelope"
              }
            },
            "controller_evidence_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "disposition_set": {
              "$ref": "#/$defs/ArtifactDispositionSetIdentityDocument"
            },
            "execution_envelope_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "execution_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "execution_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "format": {
              "const": "riverhog-collection-derivation/v1",
              "type": "string"
            },
            "input_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation": {
              "$ref": "#/$defs/OperationIdentityDocument"
            },
            "recipe": {
              "$ref": "#/$defs/RecipeIdentityDocument"
            }
          },
          "required": [
            "format",
            "execution_id",
            "claim",
            "recipe",
            "operation",
            "input_set_sha256",
            "artifact_set_sha256",
            "execution_envelope_sha256",
            "execution_sha256",
            "controller_evidence",
            "controller_evidence_sha256",
            "disposition_set"
          ],
          "type": "object"
        },
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
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        },
        "OperationIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256"
          ],
          "type": "object"
        },
        "RecipeIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "revision": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "revision",
            "sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "derivation": {
          "$ref": "#/$defs/CollectionDerivationDocument"
        },
        "document_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "document_sha256",
        "derivation"
      ],
      "type": "object"
    },
    "signature": "\"(*, collection_id: CollectionId, document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], derivation: riverhog_protocol.collection_workflow_transport.CollectionDerivationDocument) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionDerivationResponseDocument",
  "unit": "export"
}
```

</details>
