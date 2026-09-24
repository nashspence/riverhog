# stove0_operator_contracts.SchedulerRunRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerrunrequest:363e54dbf7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fe64cefde7"></a>
- <a id="s-2cde27058c"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-9de2d51403"></a>`module`: `stove0_operator_contracts`
- <a id="s-ad02fd17ea"></a>`name`: `SchedulerRunRequest`
- <a id="s-8768c4360e"></a>`unit`: `export`

### Declared structure

- <a id="s-e933dba8d5"></a>`kind`: `"class"`
- <a id="s-ff7bca2565"></a>`signature`: `"\"(*, role: Literal['controller', 'worker', 'combined'] = 'combined', work_limit: Annotated[int, Ge(ge=1), Le(le=100)] = 25) -> None\""`

#### Validated model schema

<a id="s-5c3db94817"></a>

- <a id="s-7cbdab53a7"></a>`type`: `"object"`
- <a id="s-f5b17d387a"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1a5baf21cd"></a>`role` | no | type="string"; enum=["controller","worker","combined"]; default="combined" |  |
| <a id="s-e61d11049f"></a>`work_limit` | no | type="integer"; minimum=1; maximum=100; default=25 |  |

## Governing policies

- <a id="pa-aa43a769e6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerRunRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22070d51fbaa54c92a6fe0b2c123cd28cc3564b5e492b596dc19b2bf716c6c0e -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "role": {
          "default": "combined",
          "enum": [
            "controller",
            "worker",
            "combined"
          ],
          "type": "string"
        },
        "work_limit": {
          "default": 25,
          "maximum": 100,
          "minimum": 1,
          "type": "integer"
        }
      },
      "type": "object"
    },
    "signature": "\"(*, role: Literal['controller', 'worker', 'combined'] = 'combined', work_limit: Annotated[int, Ge(ge=1), Le(le=100)] = 25) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerRunRequest",
  "unit": "export"
}
```

</details>
