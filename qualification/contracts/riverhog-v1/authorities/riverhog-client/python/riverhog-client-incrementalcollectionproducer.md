# riverhog_client.IncrementalCollectionProducer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-incrementalcollectionproducer:6d0cd61a2d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf3a9d8781"></a>
- <a id="s-8030e5d2ff"></a>`distribution`: `riverhog-client`
- <a id="s-f611532634"></a>`module`: `riverhog_client`
- <a id="s-6e81ab5019"></a>`name`: `IncrementalCollectionProducer`
- <a id="s-5c906749d3"></a>`unit`: `export`

### Declared structure

- <a id="s-793bc55af1"></a>`kind`: `"class"`
- <a id="s-df3b24c778"></a>`signature`: `"'(api: \\'ApiClient\\', *, producer_app: \\'str\\', adapter_id: \\'str\\', adapter_version: \\'str\\', ingest_source: \\'str\\', source_event_id: \\'str\\', source_context: \\'Mapping[str, object] \| None\\' = None, idempotency_key: \\'str \| None\\' = None, archive_store: \\'ArchiveStoreName \| None\\' = None, description: \\'CollectionDescription \| None\\' = None, tags: \\'Sequence[CollectionTag]\\' = (), event_context: \\'Mapping[str, object] \| None\\' = None, provenance_mode: \"Literal[\\'captured\\', \\'omitted\\']\" = \\'omitted\\', server_generated_provenance: \\'bool\\' = False, provenance_omission_reason: \\'str\\' = \\'Producer did not receive host provenance; immutable producer evidence records the source boundary.\\', progress: \\'ReadProgress \| None\\' = None) -> \\'None\\''"`

## Maintained corroboration

### Related interface records

- [append_inputs](riverhog-client-incrementalcollectionproducer-append-inputs.md)
- [heartbeat](riverhog-client-incrementalcollectionproducer-heartbeat.md)
- [stage_provenance_journals](riverhog-client-incrementalcollectionproducer-stage-provenance-journals.md)
- [append_derivation_evidence](riverhog-client-incrementalcollectionproducer-append-derivation-evidence.md)
- [finish](riverhog-client-incrementalcollectionproducer-finish.md)
- [stop](riverhog-client-incrementalcollectionproducer-stop.md)

## Governing policies

- <a id="pa-c768ffc13e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.IncrementalCollectionProducer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0644be8e37f3c7a39edfc7af4c90b0ed614111fc33679df9389df1c86ab97813 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(api: \\'ApiClient\\', *, producer_app: \\'str\\', adapter_id: \\'str\\', adapter_version: \\'str\\', ingest_source: \\'str\\', source_event_id: \\'str\\', source_context: \\'Mapping[str, object] | None\\' = None, idempotency_key: \\'str | None\\' = None, archive_store: \\'ArchiveStoreName | None\\' = None, description: \\'CollectionDescription | None\\' = None, tags: \\'Sequence[CollectionTag]\\' = (), event_context: \\'Mapping[str, object] | None\\' = None, provenance_mode: \"Literal[\\'captured\\', \\'omitted\\']\" = \\'omitted\\', server_generated_provenance: \\'bool\\' = False, provenance_omission_reason: \\'str\\' = \\'Producer did not receive host provenance; immutable producer evidence records the source boundary.\\', progress: \\'ReadProgress | None\\' = None) -> \\'None\\''"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "IncrementalCollectionProducer",
  "unit": "export"
}
```

</details>
