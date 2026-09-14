# riverhog_client.CatalogReplica.page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogreplica-page:03c861c06e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7c95aaede0"></a>
- <a id="s-cd7d0c8c1a"></a>`distribution`: `riverhog-client`
- <a id="s-53bf2e46f3"></a>`module`: `riverhog_client`
- <a id="s-ce7f01bfb6"></a>`name`: `page`
- <a id="s-ffbba68c9e"></a>`owner`: `riverhog_client.CatalogReplica`
- <a id="s-81fa750fea"></a>`unit`: `member`

### Declared structure

- <a id="s-fcddf1b4f0"></a>`kind`: `"method"`
- <a id="s-9387a7c592"></a>`signature`: `"\"(self, *, after: 'int' = 0, limit: 'int' = 100, tags: 'Sequence[str]' = ()) -> 'list[CatalogSyncDescriptor]'\""`

## Maintained corroboration

### Related interface records

- [CatalogReplica](riverhog-client-catalogreplica.md)

## Governing policies

- <a id="pa-e09e7f319c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CatalogReplica.page`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4c090de9480670a36186afc4a7873e515d8b8c0571f8b3de371b101bec36f21 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, after: 'int' = 0, limit: 'int' = 100, tags: 'Sequence[str]' = ()) -> 'list[CatalogSyncDescriptor]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "page",
  "owner": "riverhog_client.CatalogReplica",
  "unit": "member"
}
```
