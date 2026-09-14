# http_api_contracts.BrowseTokenCodec

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-browsetokencodec:3808ddd974 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d677806ed6"></a>
- <a id="s-5b488df88b"></a>`distribution`: `http-api-contracts`
- <a id="s-0d7bc776ee"></a>`module`: `http_api_contracts`
- <a id="s-b2291e5a5a"></a>`name`: `BrowseTokenCodec`
- <a id="s-5db93dd9f7"></a>`unit`: `export`

### Declared structure

- <a id="s-547626b272"></a>`kind`: `"class"`
- <a id="s-0e13a06a5b"></a>`signature`: `"\"(signing_key: 'str \| bytes', *, lifetime_seconds: 'int', clock: 'Callable[[], float]' = <built-in function time>) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [http_api_contracts.BrowseTokenCodec.issue](http-api-contracts-browsetokencodec-issue.md)
- [http_api_contracts.BrowseTokenCodec.verify](http-api-contracts-browsetokencodec-verify.md)

## Governing policies

- <a id="pa-fdb71ad5b1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.BrowseTokenCodec`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0903ad1fc7b9d711434d53833980e3e3db593ca917f60329a274a0bc00d36ab8 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(signing_key: 'str | bytes', *, lifetime_seconds: 'int', clock: 'Callable[[], float]' = <built-in function time>) -> 'None'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "BrowseTokenCodec",
  "unit": "export"
}
```
