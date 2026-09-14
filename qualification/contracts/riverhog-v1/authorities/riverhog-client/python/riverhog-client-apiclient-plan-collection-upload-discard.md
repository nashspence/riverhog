# riverhog_client.ApiClient.plan_collection_upload_discard

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-plan-collection-acf475fcf6:11cf94ba14 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-15df80556a"></a>
- <a id="s-aaf88ff0bc"></a>`distribution`: `riverhog-client`
- <a id="s-c9a1d85cce"></a>`module`: `riverhog_client`
- <a id="s-fd24e2ac8d"></a>`name`: `plan_collection_upload_discard`
- <a id="s-c4aa030bb8"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-8e0dabe208"></a>`unit`: `member`

### Declared structure

- <a id="s-19b1d6a199"></a>`kind`: `"method"`
- <a id="s-7c8236e22c"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-f43a7a1400"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.plan_collection_upload_discard`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 287086d6e3039c5f2b91cebcf6a6bf7f8ac41be13df2cd29df02bce8b7307e84 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "plan_collection_upload_discard",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
