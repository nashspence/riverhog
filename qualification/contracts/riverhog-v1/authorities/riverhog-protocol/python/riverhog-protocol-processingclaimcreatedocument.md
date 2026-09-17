# riverhog_protocol.ProcessingClaimCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimcreatedocument:d17840f395 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-425c1feb0b"></a>
- <a id="s-27797de284"></a>`distribution`: `riverhog-protocol`
- <a id="s-e511f932c5"></a>`module`: `riverhog_protocol`
- <a id="s-0d5d113d4f"></a>`name`: `ProcessingClaimCreateDocument`
- <a id="s-317a90dfc4"></a>`unit`: `export`

### Declared structure

- <a id="s-8f5ac273b0"></a>`kind`: `"class"`
- <a id="s-af47c58e99"></a>`signature`: `"\"(*, work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], work_document: dict[str, typing.Any], work_document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800, purpose: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)] = 'collection-work/v1') -> None\""`

#### Validated model schema

<a id="s-066637d859"></a>

- <a id="s-85e8f017f0"></a>`type`: `"object"`
- <a id="s-24777272ac"></a>`additionalProperties`: `false`
- <a id="s-e8e063f2a6"></a>`required`: `["work_id","work_document","work_document_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-54899f9616"></a>`lease_seconds` | no | type="integer"; minimum=30; maximum=86400; default=1800 |  |
| <a id="s-2300784e92"></a>`purpose` | no | type="string"; default="collection-work/v1"; maxLength=160; minLength=1 |  |
| <a id="s-7279ed4f97"></a>`work_document` | yes | type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4194304; x-riverhog-extent={"policy":"contract_max","reason":"bounded-work-document-envelope"} |  |
| <a id="s-3a0a371c33"></a>`work_document_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1a257e4bf5"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-processingclaimcreatedocument-getitem.md)
- [validate_claim](riverhog-protocol-processingclaimcreatedocument-validate-claim.md)
- [get](riverhog-protocol-processingclaimcreatedocument-get.md)

## Governing policies

- <a id="pa-835b9b4a19"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimCreateDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34961f32bfc097767ce043a17d1ba3b9462310c2ab3e356f25e586c39e3fc0f9 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "lease_seconds": {
          "default": 1800,
          "maximum": 86400,
          "minimum": 30,
          "type": "integer"
        },
        "purpose": {
          "default": "collection-work/v1",
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "work_document": {
          "additionalProperties": true,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 4194304,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-work-document-envelope"
          }
        },
        "work_document_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "work_id",
        "work_document",
        "work_document_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], work_document: dict[str, typing.Any], work_document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800, purpose: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)] = 'collection-work/v1') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimCreateDocument",
  "unit": "export"
}
```

</details>
