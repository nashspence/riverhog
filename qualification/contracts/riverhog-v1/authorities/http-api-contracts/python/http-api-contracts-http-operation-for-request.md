# http_api_contracts.http_operation_for_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-http-operation-for-request:e0919af7f4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c5c10ae2f6"></a>
- <a id="s-09a33cb497"></a>`distribution`: `http-api-contracts`
- <a id="s-d5aacd5c39"></a>`module`: `http_api_contracts`
- <a id="s-6765787a1c"></a>`name`: `http_operation_for_request`
- <a id="s-1672ec35b4"></a>`unit`: `export`

### Declared structure

- <a id="s-71795e812b"></a>`kind`: `"function"`
- <a id="s-57f07506a4"></a>`signature`: `"\"(contracts: 'tuple[HttpOperationContract, ...]', method: 'str', path: 'str') -> 'HttpOperationContract \| None'\""`

## Governing policies

- <a id="pa-25ddcc04ea"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.http_operation_for_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77abc6285e78b0f9c80dbda0a1da3923bb311d517d6a5803200bf059d2f1360d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(contracts: 'tuple[HttpOperationContract, ...]', method: 'str', path: 'str') -> 'HttpOperationContract | None'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "http_operation_for_request",
  "unit": "export"
}
```

</details>
