# http_api_contracts.HttpBodyKind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httpbodykind:6ec397b575 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31d06fe39e"></a>
- <a id="s-56dee8e2be"></a>`distribution`: `http-api-contracts`
- <a id="s-09fd80d1b1"></a>`module`: `http_api_contracts`
- <a id="s-6da36b80c4"></a>`name`: `HttpBodyKind`
- <a id="s-0696917d3e"></a>`unit`: `export`

### Declared structure

- <a id="s-abaa595689"></a>`kind`: `"object"`
- <a id="s-46d7526ccd"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-22041d8f6f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.HttpBodyKind`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1997a219ae11f089c0a1bdcac8966dd5e44576c8dc37df69edfe7d1581679bd1 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "HttpBodyKind",
  "unit": "export"
}
```

</details>
