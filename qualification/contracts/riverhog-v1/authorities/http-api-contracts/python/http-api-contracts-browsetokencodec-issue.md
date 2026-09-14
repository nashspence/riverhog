# http_api_contracts.BrowseTokenCodec.issue

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-browsetokencodec-issue:8d69fce720 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6926a79d1e"></a>
- <a id="s-b2e18c5279"></a>`distribution`: `http-api-contracts`
- <a id="s-388bde1283"></a>`module`: `http_api_contracts`
- <a id="s-199ebc5c54"></a>`name`: `issue`
- <a id="s-5b5cc624b5"></a>`owner`: `http_api_contracts.BrowseTokenCodec`
- <a id="s-e93aa00a70"></a>`unit`: `member`

### Declared structure

- <a id="s-68cbb77b4d"></a>`kind`: `"method"`
- <a id="s-034a6796c3"></a>`signature`: `"\"(self, *, operation: 'str', principal: 'object', selectors: 'Mapping[str, object]', position: 'Sequence[BrowseScalar]') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [http_api_contracts.BrowseTokenCodec](http-api-contracts-browsetokencodec.md)

## Governing policies

- <a id="pa-f676784ca5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.BrowseTokenCodec.issue`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 551e567937bad547c12174c7b01a7e5ef745cdd73d4614232078320aa18235db -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, operation: 'str', principal: 'object', selectors: 'Mapping[str, object]', position: 'Sequence[BrowseScalar]') -> 'str'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "issue",
  "owner": "http_api_contracts.BrowseTokenCodec",
  "unit": "member"
}
```
