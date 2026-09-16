# http_api_contracts.BrowseQuery

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-browsequery:5c59222695 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c6be5efbc6"></a>
- <a id="s-024093c891"></a>`distribution`: `http-api-contracts`
- <a id="s-d182ba93b7"></a>`module`: `http_api_contracts`
- <a id="s-f41da24663"></a>`name`: `BrowseQuery`
- <a id="s-7f8f1b0c00"></a>`unit`: `export`

### Declared structure

- <a id="s-7bb8fddbdf"></a>`kind`: `"type-alias"`
- <a id="s-a4ba8027aa"></a>`value`: `"typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern='^\\\\S(?:[\\\\s\\\\S]*\\\\S)?$', ascii_only=None), AfterValidator(func=<function validate_browse_query>)]"`

## Governing policies

- <a id="pa-748cf1852f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.BrowseQuery`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f2b8258bf89a030d5efcca453a18e2f3c40d515e9f85c7013b0f3484d054d6e -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern='^\\\\S(?:[\\\\s\\\\S]*\\\\S)?$', ascii_only=None), AfterValidator(func=<function validate_browse_query>)]"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "BrowseQuery",
  "unit": "export"
}
```

</details>
