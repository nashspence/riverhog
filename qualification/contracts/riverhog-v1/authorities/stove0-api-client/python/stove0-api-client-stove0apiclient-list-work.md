# stove0_api_client.Stove0ApiClient.list_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-list-work:22bf27175e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-69dbba0617"></a>
- <a id="s-1b6d389222"></a>`distribution`: `stove0-api-client`
- <a id="s-8d0e42e38b"></a>`module`: `stove0_api_client`
- <a id="s-82793badcd"></a>`name`: `list_work`
- <a id="s-ddf4aa0d74"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-f2a7ba40b0"></a>`unit`: `member`

### Declared structure

- <a id="s-bad7a73e00"></a>`kind`: `"method"`
- <a id="s-eaece0e346"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None, phase: 'WorkPhase \| None' = None, query: 'str \| None' = None, sort: 'WorkSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'WorkPage'\""`

## Maintained corroboration

### Related interface records

- [stove0_api_client.Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-a06a1cc9c9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.list_work`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f683ebb18825e3aeea8cd0ee9ea0b71753602596a55be34cef00966dda712b2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, phase: 'WorkPhase | None' = None, query: 'str | None' = None, sort: 'WorkSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'WorkPage'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "list_work",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
