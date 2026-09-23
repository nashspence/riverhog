# riverhog_protocol.ProcessingClaimOutcomesSettleDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimoutcomes-ced5cc25fd:edc78d3351 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-86ef45c1c4"></a>
- <a id="s-c38840d457"></a>`distribution`: `riverhog-protocol`
- <a id="s-4a37585a48"></a>`module`: `riverhog_protocol`
- <a id="s-702b52e103"></a>`name`: `ProcessingClaimOutcomesSettleDocument`
- <a id="s-7a6fcbbcd5"></a>`unit`: `export`

### Declared structure

- <a id="s-7732489520"></a>`kind`: `"class"`
- <a id="s-fe913c20e8"></a>`signature`: `"\"(*, fence: Annotated[NonnegativeDecimal, Ge(ge=1)], retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[NonnegativeDecimal, Ge(ge=0)] = <factory>) -> None\""`

#### Validated model schema

<a id="s-1deff4c44c"></a>

- <a id="s-da4e21848d"></a>`type`: `"object"`
- <a id="s-22ce6bb1d9"></a>`additionalProperties`: `false`
- `if`: [See `if`](#s-de20edc671)
- <a id="s-a188a875c9"></a>`required`: `["fence"]`
- `then`: [See `then`](#s-7540967b77)

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25bd6cd690"></a>`fence` | yes | [NonnegativeDecimal](#s-888b24baa5); ge=1 |  |
| <a id="s-f582d95d9d"></a>`retirement_grace_seconds` | no | [NonnegativeDecimal](#s-888b24baa5); ge=0 |  |
| <a id="s-75cba9ffc1"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |

##### Definitions

- [NonnegativeDecimal](#s-888b24baa5)

##### <a id="s-de20edc671"></a>`if`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e749e98902"></a>`retirement_policy` | no | const="retain" |  |

##### <a id="s-7540967b77"></a>`then`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c1710db3f2"></a>`retirement_grace_seconds` | no | const="0" |  |

##### <a id="s-888b24baa5"></a>definition `NonnegativeDecimal`

- <a id="s-c0ddb43ea4"></a>`type`: `"string"`
- <a id="s-f2090386e4"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [validate_outcomes](riverhog-protocol-processingclaimoutcomessettledocument-validate-outcomes.md)
- [__getitem__](riverhog-protocol-processingclaimoutcomessettledocument-getitem.md)
- [get](riverhog-protocol-processingclaimoutcomessettledocument-get.md)

## Governing policies

- <a id="pa-19e6f7fcd1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimOutcomesSettleDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52ae1ef9655c09ef8c3f327bc696a3eb7caf16615753b57e7a224999957ed83c -->

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
      "if": {
        "properties": {
          "retirement_policy": {
            "const": "retain"
          }
        }
      },
      "properties": {
        "fence": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
        },
        "retirement_grace_seconds": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 0
        },
        "retirement_policy": {
          "default": "retain",
          "enum": [
            "retain",
            "retire-after-verified-output"
          ],
          "type": "string"
        }
      },
      "required": [
        "fence"
      ],
      "then": {
        "properties": {
          "retirement_grace_seconds": {
            "const": "0"
          }
        }
      },
      "type": "object"
    },
    "signature": "\"(*, fence: Annotated[NonnegativeDecimal, Ge(ge=1)], retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[NonnegativeDecimal, Ge(ge=0)] = <factory>) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimOutcomesSettleDocument",
  "unit": "export"
}
```

</details>
