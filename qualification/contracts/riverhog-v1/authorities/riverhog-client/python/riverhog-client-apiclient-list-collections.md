# riverhog_client.ApiClient.list_collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-collections:2c40fa37ff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b5d98b310"></a>
| Field | Shape |
|---|---|
| <a id="s-892e034384"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-49414b7c0a"></a>`distribution` | "riverhog-client" |
| <a id="s-fffeb8bd10"></a>`module` | "riverhog_client" |
| <a id="s-983479ea99"></a>`name` | "list_collections" |
| <a id="s-341faaf2e8"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-6c327123ec"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-6b4c6b34d7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_collections`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a3fec9af5c2da983ad27484898f665c4131a2bb4f5db22963520afa829765d7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, tags: 'Sequence[CollectionTag]' = (), encryption_format: 'str | None' = None, passphrase_id: 'str | None' = None, sort: 'CollectionSort' = 'id', order: 'SortOrder' = 'asc') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_collections",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
