# riverhog_client.transform.ClaimedCollectionReader.replace_api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-d1a4b631fe:bc11e177ed -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-684a319c30"></a>
- <a id="s-ad2d3e0dec"></a>`distribution`: `riverhog-client`
- <a id="s-610db1f51d"></a>`module`: `riverhog_client.transform`
- <a id="s-3400c4cd64"></a>`name`: `replace_api`
- <a id="s-743331ffae"></a>`owner`: `riverhog_client.transform.ClaimedCollectionReader`
- <a id="s-8e757a0981"></a>`unit`: `member`

### Declared structure

- <a id="s-9b2a860e14"></a>`kind`: `"method"`
- <a id="s-497456ff0a"></a>`signature`: `"\"(self, api: 'ClaimedCollectionApi') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionReader](riverhog-client-transform-claimedcollectionreader.md)

## Governing policies

- <a id="pa-92a419f913"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionReader.replace_api`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 464237ad94a53a5eeaa8dd9368fe752fec46a366428c6b78e428d275efb15857 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, api: 'ClaimedCollectionApi') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "replace_api",
  "owner": "riverhog_client.transform.ClaimedCollectionReader",
  "unit": "member"
}
```
