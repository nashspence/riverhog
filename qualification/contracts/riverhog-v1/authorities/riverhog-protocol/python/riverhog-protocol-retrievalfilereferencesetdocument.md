# riverhog_protocol.RetrievalFileReferenceSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalfilereferencesetdocument:87cb8658a6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1115155719"></a>
- <a id="s-59bf0e8cee"></a>`distribution`: `riverhog-protocol`
- <a id="s-1c628d835b"></a>`module`: `riverhog_protocol`
- <a id="s-a2a199460a"></a>`name`: `RetrievalFileReferenceSetDocument`
- <a id="s-ce9fd8f62c"></a>`unit`: `export`

### Declared structure

- <a id="s-0c07729b51"></a>`kind`: `"class"`
- <a id="s-647b32cae2"></a>`signature`: `"'(*, files: Annotated[list[riverhog_protocol.retrieval_transport.RetrievalFileReferenceDocument], MinLen(min_length=1), MaxLen(max_length=10000)]) -> None'"`

#### Validated model schema

<a id="s-379ee065cb"></a>

- <a id="s-b3278f72da"></a>`type`: `"object"`
- <a id="s-474fc5b7da"></a>`additionalProperties`: `false`
- <a id="s-51a89a330a"></a>`required`: `["files"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-968becece6"></a>`files` | yes | type="array"; items=([RetrievalFileReferenceDocument](#s-221613439d)); maxItems=10000; minItems=1; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"multiple-retrieval-jobs","reason":"bounded-retrieval-work-request"} |  |

##### Definitions

- [CanonicalRelPath](#s-1828163c8e)
- [CollectionId](#s-e440dc7980)
- [RetrievalFileReferenceDocument](#s-221613439d)

##### <a id="s-1828163c8e"></a>definition `CanonicalRelPath`

- <a id="s-915b3a9da3"></a>`type`: `"string"`
- <a id="s-10d691c19a"></a>`format`: `"riverhog-canonical-relpath-v1"`
- <a id="s-eb0b5bd178"></a>`maxLength`: `4096`
- <a id="s-8936d6641d"></a>`minLength`: `1`
- <a id="s-c2a6398846"></a>`pattern`: `"^[^/\\\\]+(?:/[^/\\\\]+)*$"`
- <a id="s-444684c321"></a>`x-unicode-normalization`: `"NFC"`

###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-90c917009f"></a>1 | not=(pattern="(?:^\|/)\\.{1,2}(?:/\|$)") |
| <a id="s-4801cbe113"></a>2 | not=(pattern="^\\s\|\\s$") |

##### <a id="s-e440dc7980"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-d7fb761ade"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-a2a2e52152"></a>2 | not=(const="0") |

##### <a id="s-221613439d"></a>definition `RetrievalFileReferenceDocument`

- <a id="s-ad07143daa"></a>`type`: `"object"`
- <a id="s-236a48ce3f"></a>`additionalProperties`: `false`
- <a id="s-30804f1ab7"></a>`required`: `["collection_id","path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a556096b60"></a>`collection_id` | yes | [CollectionId](#s-e440dc7980) |  |
| <a id="s-4fcc8b9428"></a>`path` | yes | [CanonicalRelPath](#s-1828163c8e) |  |

## Maintained corroboration

### Related interface records

- [validate_exact_reference_set](riverhog-protocol-retrievalfilereferencesetdocument-validate-exact-reference-set.md)

## Governing policies

- <a id="pa-5fe3c33ae8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalFileReferenceSetDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cda2a88736ee07b57d5b489f649f5c54261a0cd85a6e3e853c85b173eb5db0cf -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CanonicalRelPath": {
          "allOf": [
            {
              "not": {
                "pattern": "(?:^|/)\\.{1,2}(?:/|$)"
              }
            },
            {
              "not": {
                "pattern": "^\\s|\\s$"
              }
            }
          ],
          "format": "riverhog-canonical-relpath-v1",
          "maxLength": 4096,
          "minLength": 1,
          "pattern": "^[^/\\\\]+(?:/[^/\\\\]+)*$",
          "type": "string",
          "x-unicode-normalization": "NFC"
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
        "RetrievalFileReferenceDocument": {
          "additionalProperties": false,
          "properties": {
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "path": {
              "$ref": "#/$defs/CanonicalRelPath"
            }
          },
          "required": [
            "collection_id",
            "path"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "files": {
          "items": {
            "$ref": "#/$defs/RetrievalFileReferenceDocument"
          },
          "maxItems": 10000,
          "minItems": 1,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "multiple-retrieval-jobs",
            "reason": "bounded-retrieval-work-request"
          }
        }
      },
      "required": [
        "files"
      ],
      "type": "object"
    },
    "signature": "'(*, files: Annotated[list[riverhog_protocol.retrieval_transport.RetrievalFileReferenceDocument], MinLen(min_length=1), MaxLen(max_length=10000)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetrievalFileReferenceSetDocument",
  "unit": "export"
}
```

</details>
