# http_api_contracts.parse_error_payload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-parse-error-payload:2e061a001b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-97d3de90ce"></a>
- <a id="s-d1f4e99f1d"></a>`distribution`: `http-api-contracts`
- <a id="s-9e6426c574"></a>`module`: `http_api_contracts`
- <a id="s-621f05de56"></a>`name`: `parse_error_payload`
- <a id="s-2ca76e0e34"></a>`unit`: `export`

### Declared structure

- <a id="s-cef752506e"></a>`kind`: `"function"`
- <a id="s-524686c026"></a>`signature`: `"\"(payload: 'object', *, fallback_message: 'str') -> 'tuple[str, str, dict[str, Any]]'\""`

## Governing policies

- <a id="pa-7f9b6df531"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.parse_error_payload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be35b07bd41506b74a2e5448ba74ae03441bdb358c1f6146a67661aeabff64f5 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(payload: 'object', *, fallback_message: 'str') -> 'tuple[str, str, dict[str, Any]]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "parse_error_payload",
  "unit": "export"
}
```

</details>
