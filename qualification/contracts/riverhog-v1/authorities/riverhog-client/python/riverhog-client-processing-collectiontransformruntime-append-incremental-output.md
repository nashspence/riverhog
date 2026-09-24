# riverhog_client.processing.CollectionTransformRuntime.append_incremental_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-collectiontran-820817d4fe:568991fa53 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8b87aea5c8"></a>
- <a id="s-f2f88a6b04"></a>`distribution`: `riverhog-client`
- <a id="s-9d4f8b4c8b"></a>`module`: `riverhog_client.processing`
- <a id="s-52ac077507"></a>`name`: `append_incremental_output`
- <a id="s-a1c18febdb"></a>`owner`: `riverhog_client.processing.CollectionTransformRuntime`
- <a id="s-7f5a2df0b1"></a>`unit`: `member`

### Declared structure

- <a id="s-a9c5709781"></a>`kind`: `"method"`
- <a id="s-af9c90b99d"></a>`signature`: `"\"(self, writer: 'IncrementalDerivedCollectionWriter', source: 'ProducerInput', *, identity: 'ProducerArtifactIdentity') -> 'tuple[ProducerArtifactCustody, ...]'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-processing-collectiontransformruntime.md)

## Governing policies

- <a id="pa-a695652e97"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CollectionTransformRuntime.append_incremental_output`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7706516904c5ff462089955c3f9fc18dadc65de1c289747393decdefccd4939 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, writer: 'IncrementalDerivedCollectionWriter', source: 'ProducerInput', *, identity: 'ProducerArtifactIdentity') -> 'tuple[ProducerArtifactCustody, ...]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "append_incremental_output",
  "owner": "riverhog_client.processing.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
