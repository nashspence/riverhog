# riverhog_protocol.ProcessingClaimAbandonDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimabandondocument:7cabede30a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f2a0cd77c2"></a>
- <a id="s-c61ad7270b"></a>`distribution`: `riverhog-protocol`
- <a id="s-881d4698c2"></a>`module`: `riverhog_protocol`
- <a id="s-64309115a1"></a>`name`: `ProcessingClaimAbandonDocument`
- <a id="s-e9841ed690"></a>`unit`: `export`

### Declared structure

- <a id="s-b2de200749"></a>`kind`: `"class"`
- <a id="s-429fb78411"></a>`signature`: `"'(*, fence: Annotated[int, Ge(ge=1)], reason: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"`

#### Validated model schema

<a id="s-dcd71186ec"></a>
- <a id="s-46ae7b32f3"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a8233096bd"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-929d3be474"></a>`reason` | yes | type="string"; minLength=1; maxLength=1000 |  |

## Governing policies

- <a id="pa-02b31155c2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimAbandonDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed8e8424e76221eee47aa6d0053cdb3ed95288c4b34384ee5125b42395503ffd -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "fence": {
          "minimum": 1,
          "type": "integer"
        },
        "reason": {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "fence",
        "reason"
      ],
      "type": "object"
    },
    "signature": "'(*, fence: Annotated[int, Ge(ge=1)], reason: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimAbandonDocument",
  "unit": "export"
}
```
