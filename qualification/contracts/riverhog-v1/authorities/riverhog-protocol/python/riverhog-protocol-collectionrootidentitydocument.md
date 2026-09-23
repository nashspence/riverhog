# riverhog_protocol.CollectionRootIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionrootidentitydocument:2894e47535 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b9c1e1d8ca"></a>
- <a id="s-a8298c29cb"></a>`distribution`: `riverhog-protocol`
- <a id="s-0ab7238084"></a>`module`: `riverhog_protocol`
- <a id="s-bb2a873c77"></a>`name`: `CollectionRootIdentityDocument`
- <a id="s-6bb54a8bdf"></a>`unit`: `export`

### Declared structure

- <a id="s-6948c7fb82"></a>`kind`: `"class"`
- <a id="s-5ac7c021a6"></a>`signature`: `"\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], content_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-a52bb9ed6b"></a>

- <a id="s-40e688420a"></a>`type`: `"object"`
- <a id="s-eb92f0e5ea"></a>`additionalProperties`: `false`
- <a id="s-bbdf134208"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d876a1dc7d"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e9209758b4"></a>`collection_id` | yes | [CollectionId](#s-7601e51a21) |  |
| <a id="s-f25b0ddace"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionId](#s-7601e51a21)

##### <a id="s-7601e51a21"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-1e8e7be8e0"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-98045b8724"></a>2 | not=(const="0") |

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-collectionrootidentitydocument-getitem.md)
- [validate_identity](riverhog-protocol-collectionrootidentitydocument-validate-identity.md)
- [get](riverhog-protocol-collectionrootidentitydocument-get.md)

## Governing policies

- <a id="pa-e8fe5095eb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionRootIdentityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a5e71fbaef5305df38bc9b5ac88168327f255b024fc17843a4252ddb8e150b47 -->

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
        }
      },
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
    },
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], content_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionRootIdentityDocument",
  "unit": "export"
}
```

</details>
