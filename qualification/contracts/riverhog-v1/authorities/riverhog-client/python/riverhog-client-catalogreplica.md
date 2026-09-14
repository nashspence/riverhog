# riverhog_client.CatalogReplica

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogreplica:d73d39e32e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-af43f6603a"></a>
- <a id="s-da3a6c4018"></a>`distribution`: `riverhog-client`
- <a id="s-7bcbd914a3"></a>`module`: `riverhog_client`
- <a id="s-33b215fbc6"></a>`name`: `CatalogReplica`
- <a id="s-24084a2bea"></a>`unit`: `export`

### Declared structure

- <a id="s-e0ddc54451"></a>`kind`: `"class"`
- <a id="s-3c9b764061"></a>`signature`: `"\"(database: 'str \| Path') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.CatalogReplica.get](riverhog-client-catalogreplica-get.md)
- [riverhog_client.CatalogReplica.page](riverhog-client-catalogreplica-page.md)
- [riverhog_client.CatalogReplica.reclaim](riverhog-client-catalogreplica-reclaim.md)
- [riverhog_client.CatalogReplica.start](riverhog-client-catalogreplica-start.md)
- [riverhog_client.CatalogReplica.status](riverhog-client-catalogreplica-status.md)
- [riverhog_client.CatalogReplica.step](riverhog-client-catalogreplica-step.md)
- [riverhog_client.CatalogReplica.tag_page](riverhog-client-catalogreplica-tag-page.md)

## Governing policies

- <a id="pa-e45a7267dd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CatalogReplica`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a5743389fb93b061b93ac601fe8c34b1048a744e9b2325481950c785b615a8b1 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(database: 'str | Path') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CatalogReplica",
  "unit": "export"
}
```
