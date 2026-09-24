# riverhog_client.IncrementalCollectionProducer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-incrementalcollectionproducer:6d0cd61a2d -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-df3b24c778"></a>`signature`: `"'(api: \\'ApiClient\\', *, producer_app: \\'str\\', adapter_id: \\'str\\', adapter_version: \\'str\\', ingest_source: \\'str\\', source_event_id: \\'str\\', source_context: \\'Mapping[str, object] \| None\\' = None, idempotency_key: \\'str \| None\\' = None, archive_store: \\'ArchiveStoreName \| None\\' = None, use_cache: \\'bool \| None\\' = None, copy_to: \\'Sequence[ArchiveStoreName] \| None\\' = None, description: \\'CollectionDescription \| None\\' = None, tags: \\'Sequence[CollectionTag]\\' = (), event_context: \\'Mapping[str, object] \| None\\' = None, provenance_mode: \"Literal[\\'captured\\', \\'omitted\\']\" = \\'omitted\\', server_generated_provenance: \\'bool\\' = False, provenance_omission_reason: \\'str\\' = \\'Producer did not receive host provenance; immutable producer evidence records the source boundary.\\', progress: \\'ReadProgress \| None\\' = None) -> \\'None\\''"`

## Maintained corroboration

### Related interface records

- [append_inputs](riverhog-client-incrementalcollectionproducer-append-inputs.md)
- [heartbeat](riverhog-client-incrementalcollectionproducer-heartbeat.md)
- [stage_provenance_journals](riverhog-client-incrementalcollectionproducer-stage-provenance-journals.md)
- [append_derivation_evidence](riverhog-client-incrementalcollectionproducer-append-derivation-evidence.md)
- [finish](riverhog-client-incrementalcollectionproducer-finish.md)
- [stop](riverhog-client-incrementalcollectionproducer-stop.md)

## Governing policies

- <a id="pa-c768ffc13e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.IncrementalCollectionProducer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb36d001dac024b7d031f443e0e322779dfe05f6168bbd55b6c3d019570eb08c -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(api: \\'ApiClient\\', *, producer_app: \\'str\\', adapter_id: \\'str\\', adapter_version: \\'str\\', ingest_source: \\'str\\', source_event_id: \\'str\\', source_context: \\'Mapping[str, object] | None\\' = None, idempotency_key: \\'str | None\\' = None, archive_store: \\'ArchiveStoreName | None\\' = None, use_cache: \\'bool | None\\' = None, copy_to: \\'Sequence[ArchiveStoreName] | None\\' = None, description: \\'CollectionDescription | None\\' = None, tags: \\'Sequence[CollectionTag]\\' = (), event_context: \\'Mapping[str, object] | None\\' = None, provenance_mode: \"Literal[\\'captured\\', \\'omitted\\']\" = \\'omitted\\', server_generated_provenance: \\'bool\\' = False, provenance_omission_reason: \\'str\\' = \\'Producer did not receive host provenance; immutable producer evidence records the source boundary.\\', progress: \\'ReadProgress | None\\' = None) -> \\'None\\''"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "IncrementalCollectionProducer",
  "unit": "export"
}
```

</details>
