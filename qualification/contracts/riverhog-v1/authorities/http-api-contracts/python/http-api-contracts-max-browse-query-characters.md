# http_api_contracts.MAX_BROWSE_QUERY_CHARACTERS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-max-browse-query-characters:bf4c97507c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-80066e9b32"></a>
- <a id="s-b1a5493f00"></a>`distribution`: `http-api-contracts`
- <a id="s-7dfaccf51c"></a>`module`: `http_api_contracts`
- <a id="s-cbe46d7334"></a>`name`: `MAX_BROWSE_QUERY_CHARACTERS`
- <a id="s-374f0987f3"></a>`unit`: `export`

### Declared structure

- <a id="s-e571da5525"></a>`kind`: `"constant"`
- <a id="s-96af3e28c1"></a>`value`: `4096`

## Governing policies

- <a id="pa-289ee16548"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.MAX_BROWSE_QUERY_CHARACTERS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 087515afc2b3150518889cdb677e98ee1e7e9f22df70994352fe9fcf1f560b0b -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 4096
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "MAX_BROWSE_QUERY_CHARACTERS",
  "unit": "export"
}
```

</details>
