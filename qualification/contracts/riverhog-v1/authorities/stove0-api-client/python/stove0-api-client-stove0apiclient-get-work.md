# stove0_api_client.Stove0ApiClient.get_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-get-work:8b96c2b3f8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-70dacbb6b4"></a>
- <a id="s-9b9a3aae4b"></a>`distribution`: `stove0-api-client`
- <a id="s-f1374299a6"></a>`module`: `stove0_api_client`
- <a id="s-eb485f56df"></a>`name`: `get_work`
- <a id="s-1b49b8071f"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-03f478e13c"></a>`unit`: `member`

### Declared structure

- <a id="s-bf41e71966"></a>`kind`: `"method"`
- <a id="s-5ad7945c88"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkView'\""`

## Maintained corroboration

### Related interface records

- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-b06fa063d4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.get_work`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e32985c892ba0f848e3e4fc904b90242023be235eff702a9f16597f0a3ca855e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "get_work",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
