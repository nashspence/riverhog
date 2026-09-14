# http_api_contracts.safe_http_base_url

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-safe-http-base-url:9276c828e2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6ba22c446d"></a>
- <a id="s-0ebdd822e6"></a>`distribution`: `http-api-contracts`
- <a id="s-c7871b54d1"></a>`module`: `http_api_contracts`
- <a id="s-c310e5c5f9"></a>`name`: `safe_http_base_url`
- <a id="s-5bb634ae0d"></a>`unit`: `export`

### Declared structure

- <a id="s-fe834481aa"></a>`kind`: `"function"`
- <a id="s-688e1e9d45"></a>`signature`: `"\"(value: 'str', *, setting: 'str' = 'base URL', allow_insecure_http: 'bool' = False) -> 'str'\""`

## Governing policies

- <a id="pa-3d4b4c743a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.safe_http_base_url`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 552e85ec0f193b8fff6ebf422eb70e7da7709d144c7cf617b9992af1afd9fe87 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str', *, setting: 'str' = 'base URL', allow_insecure_http: 'bool' = False) -> 'str'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "safe_http_base_url",
  "unit": "export"
}
```
