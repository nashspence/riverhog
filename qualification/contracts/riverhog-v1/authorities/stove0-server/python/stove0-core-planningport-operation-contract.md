# stove0_core.PlanningPort.operation_contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-planningport-operation-contract:a47e9bf33b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8f0012cbb6"></a>
- <a id="s-d16a3faf1c"></a>`distribution`: `stove0-server`
- <a id="s-dda8ffdcb0"></a>`module`: `stove0_core`
- <a id="s-e4b8c6512e"></a>`name`: `operation_contract`
- <a id="s-879e8d2ae5"></a>`owner`: `stove0_core.PlanningPort`
- <a id="s-0d79cd78d5"></a>`unit`: `member`

### Declared structure

- <a id="s-1be84de22c"></a>`kind`: `"method"`
- <a id="s-be95d8f406"></a>`signature`: `"\"(self, operation: 'OperationIdentityRef') -> 'OperationContract'\""`

## Maintained corroboration

### Related interface records

- [PlanningPort](stove0-core-planningport.md)

## Governing policies

- <a id="pa-eb135e4794"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.PlanningPort.operation_contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91841a6a64160b40e4765116ccc52175f7584fdfb040d25cf89948a8a11b4b69 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, operation: 'OperationIdentityRef') -> 'OperationContract'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "operation_contract",
  "owner": "stove0_core.PlanningPort",
  "unit": "member"
}
```

</details>
