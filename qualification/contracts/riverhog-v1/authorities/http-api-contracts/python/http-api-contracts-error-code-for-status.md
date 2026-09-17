# http_api_contracts.error_code_for_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-error-code-for-status:cb7eaa89a3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c2c07ab009"></a>
- <a id="s-ff033c241e"></a>`distribution`: `http-api-contracts`
- <a id="s-7fadb9a705"></a>`module`: `http_api_contracts`
- <a id="s-9b774b00f7"></a>`name`: `error_code_for_status`
- <a id="s-7b2d8b269f"></a>`unit`: `export`

### Declared structure

- <a id="s-b7e84dec37"></a>`kind`: `"function"`
- <a id="s-01d36660eb"></a>`signature`: `"\"(status: 'int') -> 'str'\""`

## Governing policies

- <a id="pa-fd0babd3af"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.error_code_for_status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f41e27607994ff73682d38f8494f84d20e00392005c6245c011cba6845fd67a1 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(status: 'int') -> 'str'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "error_code_for_status",
  "unit": "export"
}
```

</details>
