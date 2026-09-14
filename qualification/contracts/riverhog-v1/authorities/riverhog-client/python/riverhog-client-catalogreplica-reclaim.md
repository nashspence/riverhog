# riverhog_client.CatalogReplica.reclaim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogreplica-reclaim:9ebef4293d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f81feeef7a"></a>
- <a id="s-56559e8bb2"></a>`distribution`: `riverhog-client`
- <a id="s-03ab64871f"></a>`module`: `riverhog_client`
- <a id="s-500c59ef86"></a>`name`: `reclaim`
- <a id="s-333e97aac5"></a>`owner`: `riverhog_client.CatalogReplica`
- <a id="s-922e0bddd0"></a>`unit`: `member`

### Declared structure

- <a id="s-92d5174510"></a>`kind`: `"method"`
- <a id="s-08b5957395"></a>`signature`: `"\"(self, *, limit: 'int' = 100) -> 'int'\""`

## Maintained corroboration

### Related interface records

- [CatalogReplica](riverhog-client-catalogreplica.md)

## Governing policies

- <a id="pa-5406fa3287"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CatalogReplica.reclaim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e42cc40d3411a0145cf84faa97c869c04a9b4092d4abd58739e446251d0a7bc6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, limit: 'int' = 100) -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "reclaim",
  "owner": "riverhog_client.CatalogReplica",
  "unit": "member"
}
```
