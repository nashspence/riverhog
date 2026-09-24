# riverhog_client.processing.CollectionTransformRuntime.open_incremental_publication

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-collectiontran-933eec9226:d65a5f46dd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-91d7ae8e22"></a>
- <a id="s-eb86c03b18"></a>`distribution`: `riverhog-client`
- <a id="s-d9b3f0e6ff"></a>`module`: `riverhog_client.processing`
- <a id="s-c7fecbafa3"></a>`name`: `open_incremental_publication`
- <a id="s-22168b527d"></a>`owner`: `riverhog_client.processing.CollectionTransformRuntime`
- <a id="s-bddcecc292"></a>`unit`: `member`

### Declared structure

- <a id="s-91e432b6b3"></a>`kind`: `"method"`
- <a id="s-80d278ab91"></a>`signature`: `"\"(self, *, execution_envelope_sha256: 'str', source_context: 'Mapping[str, object] \| None' = None) -> 'IncrementalDerivedCollectionWriter'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-processing-collectiontransformruntime.md)

## Governing policies

- <a id="pa-474b904659"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CollectionTransformRuntime.open_incremental_publication`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6cafb7101d803bc9d3fac2ed1819264f36978b7d25403a60d98de645033dda47 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, execution_envelope_sha256: 'str', source_context: 'Mapping[str, object] | None' = None) -> 'IncrementalDerivedCollectionWriter'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "open_incremental_publication",
  "owner": "riverhog_client.processing.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
