# riverhog_client.CatalogReplica.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogreplica-get:bf169398a9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-30423c416d"></a>
- <a id="s-feb2253482"></a>`distribution`: `riverhog-client`
- <a id="s-ca3ad27450"></a>`module`: `riverhog_client`
- <a id="s-4bd05a553c"></a>`name`: `get`
- <a id="s-1a77c53308"></a>`owner`: `riverhog_client.CatalogReplica`
- <a id="s-5a9ecb2669"></a>`unit`: `member`

### Declared structure

- <a id="s-ae0f34ce3d"></a>`kind`: `"method"`
- <a id="s-4632c0718d"></a>`signature`: `"\"(self, collection_id: 'int') -> 'CatalogSyncDescriptor \| None'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.CatalogReplica](riverhog-client-catalogreplica.md)

## Governing policies

- <a id="pa-e44c47c370"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CatalogReplica.get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e978d039cc675b81330f402baa032d3aa306425ca87207ca641998921975f451 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'int') -> 'CatalogSyncDescriptor | None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get",
  "owner": "riverhog_client.CatalogReplica",
  "unit": "member"
}
```
