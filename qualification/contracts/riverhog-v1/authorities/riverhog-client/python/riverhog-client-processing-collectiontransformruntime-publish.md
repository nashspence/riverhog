# riverhog_client.processing.CollectionTransformRuntime.publish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-collectiontran-f63ad18de4:d473b0af9b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-add253b313"></a>
- <a id="s-fe56c1697a"></a>`distribution`: `riverhog-client`
- <a id="s-25ec1b04a3"></a>`module`: `riverhog_client.processing`
- <a id="s-6e46eff2aa"></a>`name`: `publish`
- <a id="s-1cdea828ad"></a>`owner`: `riverhog_client.processing.CollectionTransformRuntime`
- <a id="s-9d973605b3"></a>`unit`: `member`

### Declared structure

- <a id="s-12bcfdc890"></a>`kind`: `"method"`
- <a id="s-456f7c0b02"></a>`signature`: `"\"(self, outputs: 'Sequence[ProducerInput]', *, execution_envelope_sha256: 'str', execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', source_context: 'Mapping[str, object] \| None' = None, **kwargs: 'Any') -> 'DerivedCollectionReceipt'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-processing-collectiontransformruntime.md)

## Governing policies

- <a id="pa-1c71043c37"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CollectionTransformRuntime.publish`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a03bd980eeb61cf2f78916e0e6ce990b4c5af356c7c9ad8c6f62ea76abd069bc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, outputs: 'Sequence[ProducerInput]', *, execution_envelope_sha256: 'str', execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', source_context: 'Mapping[str, object] | None' = None, **kwargs: 'Any') -> 'DerivedCollectionReceipt'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "publish",
  "owner": "riverhog_client.processing.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
