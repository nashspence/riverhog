# http_api_contracts.error_payload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-error-payload:0a0233c015 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ab730c912b"></a>
- <a id="s-00d886ab81"></a>`distribution`: `http-api-contracts`
- <a id="s-e6eef3dbba"></a>`module`: `http_api_contracts`
- <a id="s-c7e714704a"></a>`name`: `error_payload`
- <a id="s-037d407ac8"></a>`unit`: `export`

### Declared structure

- <a id="s-4d206e81e4"></a>`kind`: `"function"`
- <a id="s-5df884d423"></a>`signature`: `"\"(*, code: 'str', message: 'str', details: 'Mapping[str, Any] \| None' = None) -> 'dict[str, Any]'\""`

## Governing policies

- <a id="pa-f44af45e58"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.error_payload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a702a4eb66f1169e3a97021f318be7e03eb6d89815de2083b9351148539976d4 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, code: 'str', message: 'str', details: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "error_payload",
  "unit": "export"
}
```

</details>
