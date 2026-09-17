# riverhog_client.CatalogReplica.tag_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogreplica-tag-page:e44ea806ed -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6c5e5b102e"></a>
- <a id="s-72a251ed51"></a>`distribution`: `riverhog-client`
- <a id="s-b6969d8b54"></a>`module`: `riverhog_client`
- <a id="s-057bc2138c"></a>`name`: `tag_page`
- <a id="s-39a94f79fc"></a>`owner`: `riverhog_client.CatalogReplica`
- <a id="s-f7733e1689"></a>`unit`: `member`

### Declared structure

- <a id="s-0a4816629f"></a>`kind`: `"method"`
- <a id="s-84bb9bcb05"></a>`signature`: `"\"(self, collection_id: 'int', *, after: 'str \| None' = None, limit: 'int' = 100) -> 'list[str]'\""`

## Maintained corroboration

### Related interface records

- [CatalogReplica](riverhog-client-catalogreplica.md)

## Governing policies

- <a id="pa-54a6050110"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogReplica.tag_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f2acb561f3e9dac287f27b62c79235a1a89b72e23f56acbd16170da1520605b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'int', *, after: 'str | None' = None, limit: 'int' = 100) -> 'list[str]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "tag_page",
  "owner": "riverhog_client.CatalogReplica",
  "unit": "member"
}
```

</details>
