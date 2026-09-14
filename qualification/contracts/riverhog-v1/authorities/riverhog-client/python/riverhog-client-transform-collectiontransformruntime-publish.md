# riverhog_client.transform.CollectionTransformRuntime.publish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-collectiontrans-5ad6b39e6c:a58a7f6d37 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bc66799b5f"></a>
- <a id="s-0b838167eb"></a>`distribution`: `riverhog-client`
- <a id="s-099dd738b7"></a>`module`: `riverhog_client.transform`
- <a id="s-eec2cf6714"></a>`name`: `publish`
- <a id="s-cd835e92c8"></a>`owner`: `riverhog_client.transform.CollectionTransformRuntime`
- <a id="s-7beb368a7d"></a>`unit`: `member`

### Declared structure

- <a id="s-ae70ebeaa0"></a>`kind`: `"method"`
- <a id="s-4e6beb3937"></a>`signature`: `"\"(self, outputs: 'Sequence[ProducerInput]', *, execution_envelope_sha256: 'str', execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', source_context: 'Mapping[str, object] \| None' = None, **kwargs: 'Any') -> 'DerivedCollectionReceipt'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.CollectionTransformRuntime](riverhog-client-transform-collectiontransformruntime.md)

## Governing policies

- <a id="pa-c9edd93b94"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CollectionTransformRuntime.publish`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 834c83352789146a176c0664817a42fed3f960773c8a87405e0d7656adac71e1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, outputs: 'Sequence[ProducerInput]', *, execution_envelope_sha256: 'str', execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', source_context: 'Mapping[str, object] | None' = None, **kwargs: 'Any') -> 'DerivedCollectionReceipt'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "publish",
  "owner": "riverhog_client.transform.CollectionTransformRuntime",
  "unit": "member"
}
```
