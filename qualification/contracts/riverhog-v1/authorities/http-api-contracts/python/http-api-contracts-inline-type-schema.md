# http_api_contracts.inline_type_schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-inline-type-schema:3c7eb4247e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-20dc5cd2bd"></a>
- <a id="s-b5b324e986"></a>`distribution`: `http-api-contracts`
- <a id="s-9099ff8b4a"></a>`module`: `http_api_contracts`
- <a id="s-cad2d546e5"></a>`name`: `inline_type_schema`
- <a id="s-bbd52e6d78"></a>`unit`: `export`

### Declared structure

- <a id="s-61c7b776f9"></a>`kind`: `"function"`
- <a id="s-ab38a98bce"></a>`signature`: `"\"(value: 'object') -> 'dict[str, Any]'\""`

## Governing policies

- <a id="pa-5d8c74e961"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.inline_type_schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7fb7a2ca7f48e81e972d1d94938690ce12baeadaf20a91ca2b59f59ca1aec509 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'dict[str, Any]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "inline_type_schema",
  "unit": "export"
}
```

</details>
