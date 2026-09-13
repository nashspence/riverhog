# riverhog_client.CollectionProducer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-collectionproducer:4f46fcd332 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3d1cd5ee41"></a>
| Field | Shape |
|---|---|
| <a id="s-3c9f337064"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-652921bea5"></a>`distribution` | "riverhog-client" |
| <a id="s-c660099922"></a>`module` | "riverhog_client" |
| <a id="s-f087c8edb3"></a>`name` | "CollectionProducer" |
| <a id="s-3b1d6ae227"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_client.CollectionProducer.publish_inputs](riverhog-client-collectionproducer-publish-inputs.md)
- [riverhog_client.CollectionProducer.publish](riverhog-client-collectionproducer-publish.md)

## Governing policies

- <a id="pa-45c38d5032"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CollectionProducer`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d02b7d5d6eaf4f4c624bc2cc30e4094884256c0ec5be49afb1c89efd872f74e7 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(api: \\'ApiClient\\', *, producer_app: \\'str\\', adapter_id: \\'str\\', adapter_version: \\'str\\', ingest_source: \\'str\\', archive_store: \\'ArchiveStoreName | None\\' = None, description: \\'CollectionDescription | None\\' = None, tags: \\'Sequence[CollectionTag]\\' = (), provenance_mode: \"Literal[\\'captured\\', \\'omitted\\']\" = \\'omitted\\', provenance_omission_reason: \\'str\\' = \\'Producer did not receive host provenance; immutable producer evidence records the source boundary.\\', server_generated_provenance: \\'bool\\' = False) -> \\'None\\''"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CollectionProducer",
  "unit": "export"
}
```
