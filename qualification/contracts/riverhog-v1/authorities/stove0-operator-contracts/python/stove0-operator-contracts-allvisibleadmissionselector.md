# stove0_operator_contracts.AllVisibleAdmissionSelector

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-allvisibleadmis-fe41d7800a:0d187115c7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7756b3642c"></a>
- <a id="s-d3374f7324"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-11540fbf17"></a>`module`: `stove0_operator_contracts`
- <a id="s-b1899fa5c9"></a>`name`: `AllVisibleAdmissionSelector`
- <a id="s-3415a8bca3"></a>`unit`: `export`

### Declared structure

- <a id="s-f240b8923a"></a>`kind`: `"class"`
- <a id="s-25dfcbbb9b"></a>`signature`: `"\"(*, kind: Literal['all'] = 'all') -> None\""`

#### Validated model schema

<a id="s-98fbbf0d75"></a>

- <a id="s-d328783040"></a>`type`: `"object"`
- <a id="s-d0926c6200"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-644362e711"></a>`kind` | no | type="string"; const="all"; default="all" |  |

## Governing policies

- <a id="pa-17f8663e65"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AllVisibleAdmissionSelector`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d49e917b8d37a8d4111a9d6c6c2a06bd491544cffa84a429b0e442c8b90dc65 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "kind": {
          "const": "all",
          "default": "all",
          "type": "string"
        }
      },
      "type": "object"
    },
    "signature": "\"(*, kind: Literal['all'] = 'all') -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AllVisibleAdmissionSelector",
  "unit": "export"
}
```

</details>
