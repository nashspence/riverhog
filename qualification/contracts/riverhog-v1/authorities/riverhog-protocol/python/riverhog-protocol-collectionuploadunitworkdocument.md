# riverhog_protocol.CollectionUploadUnitWorkDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitworkdocument:5f3207bf12 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dab1662726"></a>
- <a id="s-2e33e51c02"></a>`distribution`: `riverhog-protocol`
- <a id="s-b98beb79bc"></a>`module`: `riverhog_protocol`
- <a id="s-ae8145032e"></a>`name`: `CollectionUploadUnitWorkDocument`
- <a id="s-e700e9fd05"></a>`unit`: `export`

### Declared structure

- <a id="s-200c736c5f"></a>`kind`: `"class"`
- <a id="s-1717b69867"></a>`signature`: `"\"(*, unit: NonnegativeDecimal, payload_bytes: NonnegativeDecimal, plaintext_bytes: NonnegativeDecimal, sources: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadUnitSourceDocument], MaxLen(max_length=1000)], state: Literal['pending', 'committed']) -> None\""`

#### Validated model schema

<a id="s-525cc598ba"></a>

- <a id="s-9b92b950d9"></a>`type`: `"object"`
- <a id="s-328239323d"></a>`additionalProperties`: `false`
- <a id="s-65b35a7848"></a>`required`: `["unit","payload_bytes","plaintext_bytes","sources","state"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f093d62ec0"></a>`payload_bytes` | yes | [NonnegativeDecimal](#s-b6f25afbd2) |  |
| <a id="s-04dad5124f"></a>`plaintext_bytes` | yes | [NonnegativeDecimal](#s-b6f25afbd2) |  |
| <a id="s-a58e9ea803"></a>`sources` | yes | type="array"; items=([CollectionUploadUnitSourceDocument](#s-daf8b1a175)); maxItems=1000; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"collection-volume-sequence","reason":"bounded-upload-unit-source-map"} |  |
| <a id="s-bd038539c2"></a>`state` | yes | type="string"; enum=["pending","committed"] |  |
| <a id="s-e998ea77d3"></a>`unit` | yes | [NonnegativeDecimal](#s-b6f25afbd2) |  |

##### Definitions

- [CollectionUploadUnitSourceDocument](#s-daf8b1a175)
- [NonnegativeDecimal](#s-b6f25afbd2)

##### <a id="s-daf8b1a175"></a>definition `CollectionUploadUnitSourceDocument`

- <a id="s-1dd7b388bc"></a>`type`: `"object"`
- <a id="s-e5859c04bf"></a>`additionalProperties`: `false`
- <a id="s-c0162174a3"></a>`required`: `["path","offset","bytes","artifact_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25ac0f4560"></a>`artifact_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-41ffda13fa"></a>`bytes` | yes | [NonnegativeDecimal](#s-b6f25afbd2) |  |
| <a id="s-cd4891b241"></a>`offset` | yes | [NonnegativeDecimal](#s-b6f25afbd2) |  |
| <a id="s-5a0d3f2128"></a>`path` | yes | type="string" |  |

##### <a id="s-b6f25afbd2"></a>definition `NonnegativeDecimal`

- <a id="s-a8dd772fde"></a>`type`: `"string"`
- <a id="s-9e3e9f3656"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [validate_sources](riverhog-protocol-collectionuploadunitworkdocument-validate-sources.md)

## Governing policies

- <a id="pa-2918ee7ee4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitWorkDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02b9fb6121d98f0f11330c04f3eb79420524a1a29ff1a16a72587641852317ed -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionUploadUnitSourceDocument": {
          "additionalProperties": false,
          "properties": {
            "artifact_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "bytes": {
              "$ref": "#/$defs/NonnegativeDecimal"
            },
            "offset": {
              "$ref": "#/$defs/NonnegativeDecimal"
            },
            "path": {
              "type": "string"
            }
          },
          "required": [
            "path",
            "offset",
            "bytes",
            "artifact_sha256"
          ],
          "type": "object"
        },
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "payload_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        "plaintext_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        "sources": {
          "items": {
            "$ref": "#/$defs/CollectionUploadUnitSourceDocument"
          },
          "maxItems": 1000,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "collection-volume-sequence",
            "reason": "bounded-upload-unit-source-map"
          }
        },
        "state": {
          "enum": [
            "pending",
            "committed"
          ],
          "type": "string"
        },
        "unit": {
          "$ref": "#/$defs/NonnegativeDecimal"
        }
      },
      "required": [
        "unit",
        "payload_bytes",
        "plaintext_bytes",
        "sources",
        "state"
      ],
      "type": "object"
    },
    "signature": "\"(*, unit: NonnegativeDecimal, payload_bytes: NonnegativeDecimal, plaintext_bytes: NonnegativeDecimal, sources: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadUnitSourceDocument], MaxLen(max_length=1000)], state: Literal['pending', 'committed']) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadUnitWorkDocument",
  "unit": "export"
}
```

</details>
