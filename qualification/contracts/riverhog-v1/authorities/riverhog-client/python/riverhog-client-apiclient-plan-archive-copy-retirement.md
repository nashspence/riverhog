# riverhog_client.ApiClient.plan_archive_copy_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-plan-archive-co-6be0c76add:7b4c39a8d7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eb5098a94b"></a>
- <a id="s-a5d5609e5a"></a>`distribution`: `riverhog-client`
- <a id="s-a1ce6c590c"></a>`module`: `riverhog_client`
- <a id="s-374f7726b9"></a>`name`: `plan_archive_copy_retirement`
- <a id="s-f5eeb02596"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-f44f633fcc"></a>`unit`: `member`

### Declared structure

- <a id="s-acfb55ce9e"></a>`kind`: `"method"`
- <a id="s-05514e7a70"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, store: 'ArchiveStoreName') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-f1750bfe17"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.plan_archive_copy_retirement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f9f8467ceffefb83c7b8cb0ba90fa47ea732ba667b8eeafcaec67638d8ff204 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, store: 'ArchiveStoreName') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "plan_archive_copy_retirement",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
