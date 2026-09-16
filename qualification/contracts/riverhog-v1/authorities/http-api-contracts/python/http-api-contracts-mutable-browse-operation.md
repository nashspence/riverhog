# http_api_contracts.mutable_browse_operation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-mutable-browse-operation:b2cda3a066 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ad786a6e86"></a>
- <a id="s-0f701edc7a"></a>`distribution`: `http-api-contracts`
- <a id="s-e3b62182ae"></a>`module`: `http_api_contracts`
- <a id="s-b79a15e54d"></a>`name`: `mutable_browse_operation`
- <a id="s-a833831ef7"></a>`unit`: `export`

### Declared structure

- <a id="s-1917885bf2"></a>`kind`: `"function"`
- <a id="s-1f5ef45d1f"></a>`signature`: `"\"(*, default_page_size: 'int' = 25, maximum_page_size: 'int' = 100, response_items_field: 'str \| None' = None) -> 'dict[str, Any]'\""`

## Governing policies

- <a id="pa-389b484b1b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.mutable_browse_operation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d79352fd4908a0c33dfab44dfa5ed7f66c3762d3e0d2932592ee19f86a4b60b -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, default_page_size: 'int' = 25, maximum_page_size: 'int' = 100, response_items_field: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "mutable_browse_operation",
  "unit": "export"
}
```

</details>
