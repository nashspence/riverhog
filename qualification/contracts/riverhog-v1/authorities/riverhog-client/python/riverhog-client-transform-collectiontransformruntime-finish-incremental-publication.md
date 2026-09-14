# riverhog_client.transform.CollectionTransformRuntime.finish_incremental_publication

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-collectiontrans-ae385424d4:83bcfe366a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-847d4e3070"></a>
- <a id="s-ccbcc4b76b"></a>`distribution`: `riverhog-client`
- <a id="s-6c79049d20"></a>`module`: `riverhog_client.transform`
- <a id="s-e1d3848635"></a>`name`: `finish_incremental_publication`
- <a id="s-2233b65bb5"></a>`owner`: `riverhog_client.transform.CollectionTransformRuntime`
- <a id="s-df09733876"></a>`unit`: `member`

### Declared structure

- <a id="s-05c1d29dd3"></a>`kind`: `"method"`
- <a id="s-48936bf277"></a>`signature`: `"\"(self, writer: 'IncrementalDerivedCollectionWriter', *, execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', **kwargs: 'Any') -> 'DerivedCollectionReceipt'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.CollectionTransformRuntime](riverhog-client-transform-collectiontransformruntime.md)

## Governing policies

- <a id="pa-556171ee31"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CollectionTransformRuntime.finish_incremental_publication`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2db40c8a66668de111464c5472b2a0c79491e461a1dc8670d651d29a4f2eee90 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, writer: 'IncrementalDerivedCollectionWriter', *, execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', **kwargs: 'Any') -> 'DerivedCollectionReceipt'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "finish_incremental_publication",
  "owner": "riverhog_client.transform.CollectionTransformRuntime",
  "unit": "member"
}
```
