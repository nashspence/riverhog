# http_api_contracts.http_operation_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-http-operation-inventory:cee9831daf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31da1d6f4b"></a>
- <a id="s-ac35175b9c"></a>`distribution`: `http-api-contracts`
- <a id="s-639a5286ed"></a>`module`: `http_api_contracts`
- <a id="s-95f2244735"></a>`name`: `http_operation_inventory`
- <a id="s-bcc473c510"></a>`unit`: `export`

### Declared structure

- <a id="s-1ceea5be9e"></a>`kind`: `"function"`
- <a id="s-e6eed6a87f"></a>`signature`: `"\"(contracts: 'Sequence[HttpOperationContract]') -> 'list[dict[str, Any]]'\""`

## Governing policies

- <a id="pa-a995cb6170"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.http_operation_inventory`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d26289885a630992ddee8def8fe7278365afa7100c9f5fa7221f69eb0b32a54f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(contracts: 'Sequence[HttpOperationContract]') -> 'list[dict[str, Any]]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "http_operation_inventory",
  "unit": "export"
}
```

</details>
