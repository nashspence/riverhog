# riverhog_protocol.ProcessingOutcomeBindingDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingoutcomebindingdocument:c6ec8a4c02 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-036f962df3"></a>
- <a id="s-70a71c9379"></a>`distribution`: `riverhog-protocol`
- <a id="s-6609b97483"></a>`module`: `riverhog_protocol`
- <a id="s-c868405ca9"></a>`name`: `ProcessingOutcomeBindingDocument`
- <a id="s-2f9ad98ba9"></a>`unit`: `export`

### Declared structure

- <a id="s-8eea9d483e"></a>`kind`: `"class"`
- <a id="s-7d1b4e0abb"></a>`signature`: `"\"(*, claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[int, Ge(ge=1)], outcome_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')]) -> None\""`

#### Validated model schema

<a id="s-a5785b1757"></a>
- <a id="s-20e2ed49f4"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cc2bff4b33"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-78fcecbf52"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-280acd085f"></a>`outcome_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-processingoutcomebindingdocument-getitem.md)
- [get](riverhog-protocol-processingoutcomebindingdocument-get.md)

## Governing policies

- <a id="pa-f054bd7fb0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingOutcomeBindingDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cbf23809f969641092ddd78420b8992484827653a053d36ca5c72e7b8e572745 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "claim_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "type": "integer"
        },
        "outcome_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        }
      },
      "required": [
        "claim_id",
        "fence",
        "outcome_id"
      ],
      "type": "object"
    },
    "signature": "\"(*, claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[int, Ge(ge=1)], outcome_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingOutcomeBindingDocument",
  "unit": "export"
}
```
