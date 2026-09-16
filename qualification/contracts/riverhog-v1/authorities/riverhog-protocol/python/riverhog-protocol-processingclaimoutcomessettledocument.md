# riverhog_protocol.ProcessingClaimOutcomesSettleDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimoutcomes-ced5cc25fd:edc78d3351 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-fe913c20e8"></a>`signature`: `"\"(*, fence: Annotated[int, Ge(ge=1)], retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0) -> None\""`

#### Validated model schema

<a id="s-1deff4c44c"></a>
- <a id="s-da4e21848d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25bd6cd690"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-f582d95d9d"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0 |  |
| <a id="s-75cba9ffc1"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"] |  |

## Maintained corroboration

### Related interface records

- [validate_outcomes](riverhog-protocol-processingclaimoutcomessettledocument-validate-outcomes.md)
- [__getitem__](riverhog-protocol-processingclaimoutcomessettledocument-getitem.md)
- [get](riverhog-protocol-processingclaimoutcomessettledocument-get.md)

## Governing policies

- <a id="pa-19e6f7fcd1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimOutcomesSettleDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ef9c253ca9a7cb7769a3e2d62198195c4844981bcc240a2d2f36119ac1a66fc -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
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
          "minimum": 1,
          "type": "integer"
        },
        "retirement_grace_seconds": {
          "default": 0,
          "minimum": 0,
          "type": "integer"
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
            "const": 0
          }
        }
      },
      "type": "object"
    },
    "signature": "\"(*, fence: Annotated[int, Ge(ge=1)], retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimOutcomesSettleDocument",
  "unit": "export"
}
```
