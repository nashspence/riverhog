# riverhog_client.ApiClient.retire_archive_copy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-retire-archive-copy:58b47d1f19 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0702dde400"></a>
- <a id="s-7bea1d7edb"></a>`distribution`: `riverhog-client`
- <a id="s-723ac6318e"></a>`module`: `riverhog_client`
- <a id="s-cd47d8c532"></a>`name`: `retire_archive_copy`
- <a id="s-a4b9997a61"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-45c7250a5a"></a>`unit`: `member`

### Declared structure

- <a id="s-4ea89515a9"></a>`kind`: `"method"`
- <a id="s-2d75c2223e"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, store: 'ArchiveStoreName', challenge: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-d52918a4b4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.retire_archive_copy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90ab1ff1eaaefa80f1fbedeb1658f69b3ea0e3a9c73e4837909b602ceffd02c3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, store: 'ArchiveStoreName', challenge: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "retire_archive_copy",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
