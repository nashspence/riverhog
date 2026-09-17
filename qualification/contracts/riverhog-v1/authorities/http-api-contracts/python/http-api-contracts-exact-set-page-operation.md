# http_api_contracts.exact_set_page_operation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-exact-set-page-operation:43ec196dc4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e6e871ac45"></a>
- <a id="s-59654a5cb2"></a>`distribution`: `http-api-contracts`
- <a id="s-c2778dc22f"></a>`module`: `http_api_contracts`
- <a id="s-d6f27df0a7"></a>`name`: `exact_set_page_operation`
- <a id="s-b4e1397623"></a>`unit`: `export`

### Declared structure

- <a id="s-b772f238a8"></a>`kind`: `"function"`
- <a id="s-88152dd555"></a>`signature`: `"\"(*, authority: 'str', cursor_parameter: 'str', limit_parameter: 'str', validator_header: 'str') -> 'dict[str, Any]'\""`

## Governing policies

- <a id="pa-a1d4277692"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.exact_set_page_operation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 47625f65f10c8b18a63d12b734793daa4b655427c869b4f8aa0d7598cacaf876 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, authority: 'str', cursor_parameter: 'str', limit_parameter: 'str', validator_header: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "exact_set_page_operation",
  "unit": "export"
}
```

</details>
