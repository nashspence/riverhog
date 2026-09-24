# riverhog_client.processing.DerivedCollectionWriter.publish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-derivedcollect-faffde758e:139f67fd04 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ceea49d308"></a>
- <a id="s-9956a94f8b"></a>`distribution`: `riverhog-client`
- <a id="s-8ab1ad6e22"></a>`module`: `riverhog_client.processing`
- <a id="s-a14ad6e938"></a>`name`: `publish`
- <a id="s-501d50c133"></a>`owner`: `riverhog_client.processing.DerivedCollectionWriter`
- <a id="s-50f7cab66f"></a>`unit`: `member`

### Declared structure

- <a id="s-b34259576c"></a>`kind`: `"method"`
- <a id="s-5427f29554"></a>`signature`: `"\"(self, outputs: 'Sequence[ProducerInput]', *, execution_envelope_sha256: 'str', execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', source_context: 'Mapping[str, object] \| None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'DerivedCollectionReceipt'\""`

## Maintained corroboration

### Related interface records

- [DerivedCollectionWriter](riverhog-client-processing-derivedcollectionwriter.md)

## Governing policies

- <a id="pa-ccfb3a8a63"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.DerivedCollectionWriter.publish`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21156bed4767d360477c3156361118f67dcb10253c7d7d5ac453da7bb462060b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, outputs: 'Sequence[ProducerInput]', *, execution_envelope_sha256: 'str', execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', source_context: 'Mapping[str, object] | None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'DerivedCollectionReceipt'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "publish",
  "owner": "riverhog_client.processing.DerivedCollectionWriter",
  "unit": "member"
}
```

</details>
