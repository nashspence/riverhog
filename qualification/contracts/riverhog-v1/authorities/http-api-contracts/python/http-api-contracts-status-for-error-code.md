# http_api_contracts.status_for_error_code

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-status-for-error-code:cd71f351f7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9901bd9596"></a>
- <a id="s-1c5447a837"></a>`distribution`: `http-api-contracts`
- <a id="s-e3d8dc167d"></a>`module`: `http_api_contracts`
- <a id="s-db6b38608e"></a>`name`: `status_for_error_code`
- <a id="s-917aedca73"></a>`unit`: `export`

### Declared structure

- <a id="s-c758664cb8"></a>`kind`: `"function"`
- <a id="s-12dfa1f22a"></a>`signature`: `"\"(code: 'str', *, fallback: 'int' = 500) -> 'int'\""`

## Governing policies

- <a id="pa-c25e576605"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.status_for_error_code`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 762f86cdb3558c932566a1a44a8c32eda59e957161b0732dd2ad70ba570cf666 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(code: 'str', *, fallback: 'int' = 500) -> 'int'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "status_for_error_code",
  "unit": "export"
}
```

</details>
