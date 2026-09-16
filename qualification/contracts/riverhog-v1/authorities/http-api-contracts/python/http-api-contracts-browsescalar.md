# http_api_contracts.BrowseScalar

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-browsescalar:1336454930 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dd7682e8d0"></a>
- <a id="s-6277ea463b"></a>`distribution`: `http-api-contracts`
- <a id="s-e03aa93ef7"></a>`module`: `http_api_contracts`
- <a id="s-d3014a6f67"></a>`name`: `BrowseScalar`
- <a id="s-def2fe39b5"></a>`unit`: `export`

### Declared structure

- <a id="s-7c56da1e83"></a>`kind`: `"type-alias"`
- <a id="s-5bd69be4f7"></a>`value`: `"str \| int \| bool \| bytes \| None"`

## Governing policies

- <a id="pa-d1ca6062d2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.BrowseScalar`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f9d7bd1dc8e8ec57bec019f6a800c096ef74978bfb117ea7fbfe8aa540d3b28 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "str | int | bool | bytes | None"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "BrowseScalar",
  "unit": "export"
}
```

</details>
