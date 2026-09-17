# http_api_contracts.error_responses

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-error-responses:577af8f06a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-65b8fa4713"></a>
- <a id="s-397601351f"></a>`distribution`: `http-api-contracts`
- <a id="s-0a34762f57"></a>`module`: `http_api_contracts`
- <a id="s-c27026d023"></a>`name`: `error_responses`
- <a id="s-6e4c8be617"></a>`unit`: `export`

### Declared structure

- <a id="s-5cf0c0a986"></a>`kind`: `"function"`
- <a id="s-af57571082"></a>`signature`: `"\"(*codes: 'str') -> 'dict[int \| str, dict[str, Any]]'\""`

## Governing policies

- <a id="pa-3e006d51f1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.error_responses`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: baafd0a9ba3ef6436f34ba1189bbfc62bfef94cda7223fff5626cec3e9fab935 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*codes: 'str') -> 'dict[int | str, dict[str, Any]]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "error_responses",
  "unit": "export"
}
```

</details>
