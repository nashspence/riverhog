# riverhog_client.processing.IncrementalDerivedCollectionWriter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-incrementalder-2d91d1d3c5:97570b4b0c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-81808b79cb"></a>
- <a id="s-e3cf55b2b3"></a>`distribution`: `riverhog-client`
- <a id="s-7368d8074d"></a>`module`: `riverhog_client.processing`
- <a id="s-c820f28a18"></a>`name`: `IncrementalDerivedCollectionWriter`
- <a id="s-7adc5baefd"></a>`unit`: `export`

### Declared structure

- <a id="s-0127046ab4"></a>`kind`: `"class"`
- <a id="s-296fd4b84c"></a>`signature`: `"\"(api: 'Any', *, spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', producer_app: 'str', producer_version: 'str', execution_envelope_sha256: 'str', source_context: 'Mapping[str, object] \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [heartbeat](riverhog-client-processing-incrementalderivedcollectionwriter-heartbeat.md)
- [append](riverhog-client-processing-incrementalderivedcollectionwriter-append.md)
- [stop](riverhog-client-processing-incrementalderivedcollectionwriter-stop.md)
- [finish](riverhog-client-processing-incrementalderivedcollectionwriter-finish.md)

## Governing policies

- <a id="pa-64687622a6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.IncrementalDerivedCollectionWriter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 780f723ae3baf08b7201b645c63cdb9a22e422cc3d68b2c2792150853145d411 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'Any', *, spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', producer_app: 'str', producer_version: 'str', execution_envelope_sha256: 'str', source_context: 'Mapping[str, object] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "IncrementalDerivedCollectionWriter",
  "unit": "export"
}
```

</details>
