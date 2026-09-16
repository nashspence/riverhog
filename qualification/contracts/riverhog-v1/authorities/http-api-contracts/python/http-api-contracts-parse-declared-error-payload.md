# http_api_contracts.parse_declared_error_payload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-parse-declared-error-payload:f2446e0c41 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-209bee6e89"></a>
- <a id="s-65fe81c0e7"></a>`distribution`: `http-api-contracts`
- <a id="s-1e7cc2d006"></a>`module`: `http_api_contracts`
- <a id="s-f0ef60df13"></a>`name`: `parse_declared_error_payload`
- <a id="s-542a84296b"></a>`unit`: `export`

### Declared structure

- <a id="s-5c4ebde31e"></a>`kind`: `"function"`
- <a id="s-f2d87ae4b8"></a>`signature`: `"\"(contract: 'HttpOperationContract', *, status: 'int', payload: 'object') -> 'tuple[str, str, dict[str, Any]]'\""`

## Governing policies

- <a id="pa-459e41008d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.parse_declared_error_payload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b22c80b61c31f3415b451974d290bdeebecf73356018aa505c0b8cbdca240d6 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(contract: 'HttpOperationContract', *, status: 'int', payload: 'object') -> 'tuple[str, str, dict[str, Any]]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "parse_declared_error_payload",
  "unit": "export"
}
```

</details>
