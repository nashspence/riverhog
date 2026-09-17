# http_api_contracts.operation_interface

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-operation-interface:3499b0bce7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-72a4fe6338"></a>
- <a id="s-58ea483e11"></a>`distribution`: `http-api-contracts`
- <a id="s-d54915aa8b"></a>`module`: `http_api_contracts`
- <a id="s-d593416880"></a>`name`: `operation_interface`
- <a id="s-bb485d8836"></a>`unit`: `export`

### Declared structure

- <a id="s-93fd31cc83"></a>`kind`: `"function"`
- <a id="s-42a2746fc8"></a>`signature`: `"\"(value: 'OperationInterface') -> 'dict[str, str]'\""`

## Governing policies

- <a id="pa-03355a4631"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.operation_interface`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d6e7a0154c81a011ad4b92dfff5b6647ce77d9d4751333f7b252ec9c1bb6157c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'OperationInterface') -> 'dict[str, str]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "operation_interface",
  "unit": "export"
}
```

</details>
