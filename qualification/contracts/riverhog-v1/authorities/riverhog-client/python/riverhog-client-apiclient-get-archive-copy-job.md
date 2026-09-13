# riverhog_client.ApiClient.get_archive_copy_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-archive-copy-job:55529f3657 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c4d148755"></a>
| Field | Shape |
|---|---|
| <a id="s-62e8c15baf"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-6b737f4b9a"></a>`distribution` | "riverhog-client" |
| <a id="s-cb05a97037"></a>`module` | "riverhog_client" |
| <a id="s-6b03f222f6"></a>`name` | "get_archive_copy_job" |
| <a id="s-c46d343c0e"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-bb014bab75"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-99862fb1fc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_archive_copy_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e386a8f648c7352f466f94b5ec035f8896339012e1a4e023abfbf441f131dee6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_archive_copy_job",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
