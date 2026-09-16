# http_api_contracts.DEFAULT_HTTP_ERROR_AUTHORITY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-default-http-error-authority:219ca70fb6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5ebc797fbd"></a>
- <a id="s-3f3baa07fa"></a>`distribution`: `http-api-contracts`
- <a id="s-4cb321579b"></a>`module`: `http_api_contracts`
- <a id="s-c501b6aa17"></a>`name`: `DEFAULT_HTTP_ERROR_AUTHORITY`
- <a id="s-90547a2384"></a>`unit`: `export`

### Declared structure

- <a id="s-bbcf93aa79"></a>`kind`: `"object"`
- <a id="s-3db5b773af"></a>`type`: `"http_api_contracts.HttpOperationErrorAuthority"`

## Governing policies

- <a id="pa-64b18dd548"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.DEFAULT_HTTP_ERROR_AUTHORITY`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 36077ef3f8e3dca0b3005043ce69640005197e111128fbd7ac14da58c943d5bf -->

```json
{
  "contract": {
    "kind": "object",
    "type": "http_api_contracts.HttpOperationErrorAuthority"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "DEFAULT_HTTP_ERROR_AUTHORITY",
  "unit": "export"
}
```

</details>
