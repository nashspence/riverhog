# riverhog_client.ApiClient.list_archive_stores

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-archive-stores:8537fa550d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ef93d43352"></a>
| Field | Shape |
|---|---|
| <a id="s-90a23cf80c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-426c3e648d"></a>`distribution` | "riverhog-client" |
| <a id="s-195d9a538c"></a>`module` | "riverhog_client" |
| <a id="s-166999f83d"></a>`name` | "list_archive_stores" |
| <a id="s-9eed06b092"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-8a9abcadde"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-3032727307"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_archive_stores`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89a79ae9c0ee8549aecf2435b605e41a2bce6a1a0f9b08288e527ff86794e22e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, sort: 'ArchiveStoreSort' = 'store', order: 'SortOrder' = 'asc') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_archive_stores",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
