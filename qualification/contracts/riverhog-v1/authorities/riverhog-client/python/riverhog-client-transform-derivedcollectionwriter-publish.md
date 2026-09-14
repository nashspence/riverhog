# riverhog_client.transform.DerivedCollectionWriter.publish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-derivedcollecti-d0f6016211:2bfe2d284a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-092394fbd7"></a>
- <a id="s-804d544af8"></a>`distribution`: `riverhog-client`
- <a id="s-cf71573a65"></a>`module`: `riverhog_client.transform`
- <a id="s-7a303e1e49"></a>`name`: `publish`
- <a id="s-d427df50d5"></a>`owner`: `riverhog_client.transform.DerivedCollectionWriter`
- <a id="s-5cd14b57d0"></a>`unit`: `member`

### Declared structure

- <a id="s-c31cd52f83"></a>`kind`: `"method"`
- <a id="s-e6f23ea3ac"></a>`signature`: `"\"(self, outputs: 'Sequence[ProducerInput]', *, execution_envelope_sha256: 'str', execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', source_context: 'Mapping[str, object] \| None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'DerivedCollectionReceipt'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.DerivedCollectionWriter](riverhog-client-transform-derivedcollectionwriter.md)

## Governing policies

- <a id="pa-98c5ee5f39"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.DerivedCollectionWriter.publish`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c13c4771ed00f6c6510bfa22564537af74c6b2fc1ad5981f3b8eb0eef349abca -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, outputs: 'Sequence[ProducerInput]', *, execution_envelope_sha256: 'str', execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', source_context: 'Mapping[str, object] | None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'DerivedCollectionReceipt'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "publish",
  "owner": "riverhog_client.transform.DerivedCollectionWriter",
  "unit": "member"
}
```
