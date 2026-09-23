# riverhog_protocol.ProcessingClaimAbandonDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimabandondocument:7cabede30a -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-429fb78411"></a>`signature`: `"'(*, fence: Annotated[NonnegativeDecimal, Ge(ge=1)], reason: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"`

#### Validated model schema

<a id="s-dcd71186ec"></a>

- <a id="s-46ae7b32f3"></a>`type`: `"object"`
- <a id="s-d4429f77db"></a>`additionalProperties`: `false`
- <a id="s-3c1dee8708"></a>`required`: `["fence","reason"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a8233096bd"></a>`fence` | yes | [NonnegativeDecimal](#s-15376f9e56); ge=1 |  |
| <a id="s-929d3be474"></a>`reason` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### Definitions

- [NonnegativeDecimal](#s-15376f9e56)

##### <a id="s-15376f9e56"></a>definition `NonnegativeDecimal`

- <a id="s-feecce8071"></a>`type`: `"string"`
- <a id="s-a8dc1a4d02"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-processingclaimabandondocument-getitem.md)
- [get](riverhog-protocol-processingclaimabandondocument-get.md)

## Governing policies

- <a id="pa-02b31155c2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimAbandonDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 28f13a25771dfce2f33061f8873ef127b3e21b6be042b42f638a8ff7105b7e26 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "fence": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
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
    "signature": "'(*, fence: Annotated[NonnegativeDecimal, Ge(ge=1)], reason: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimAbandonDocument",
  "unit": "export"
}
```

</details>
