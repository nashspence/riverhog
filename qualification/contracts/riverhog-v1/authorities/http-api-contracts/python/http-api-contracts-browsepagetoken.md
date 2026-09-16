# http_api_contracts.BrowsePageToken

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-browsepagetoken:8fc12030a1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0c91a344e9"></a>
- <a id="s-a84751643f"></a>`distribution`: `http-api-contracts`
- <a id="s-a08c308ad4"></a>`module`: `http_api_contracts`
- <a id="s-08473c4e0a"></a>`name`: `BrowsePageToken`
- <a id="s-7a60c15e8d"></a>`unit`: `export`

### Declared structure

- <a id="s-c862393969"></a>`kind`: `"type-alias"`
- <a id="s-42b6794184"></a>`value`: `"typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=8192, pattern=None, ascii_only=None)]"`

## Governing policies

- <a id="pa-1786f47c67"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.BrowsePageToken`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0d33f5756a245b9afc42fec170b51453aad6b7a6fb91266e3febfd5edea6b03 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=8192, pattern=None, ascii_only=None)]"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "BrowsePageToken",
  "unit": "export"
}
```

</details>
