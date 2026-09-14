# riverhog_client.transform.CollectionTransformRuntime.prepare_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-collectiontrans-f925c10c45:0fd4b5267c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3cf9fbba7a"></a>
- <a id="s-675e1b2456"></a>`distribution`: `riverhog-client`
- <a id="s-68e32188ae"></a>`module`: `riverhog_client.transform`
- <a id="s-3c766b8e21"></a>`name`: `prepare_inputs`
- <a id="s-13bf0f07b3"></a>`owner`: `riverhog_client.transform.CollectionTransformRuntime`
- <a id="s-6108e9d75d"></a>`unit`: `member`

### Declared structure

- <a id="s-eafab4a492"></a>`kind`: `"method"`
- <a id="s-65bed17b38"></a>`signature`: `"\"(self, artifacts: 'Sequence[ClaimedArtifact] \| None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.CollectionTransformRuntime](riverhog-client-transform-collectiontransformruntime.md)

## Governing policies

- <a id="pa-b7e7f1b373"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CollectionTransformRuntime.prepare_inputs`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0d6e4b4b74aab3b9e0e1b2c4bb6a28e4c19df729b6c39f582d7e06c097b6989 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifacts: 'Sequence[ClaimedArtifact] | None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "prepare_inputs",
  "owner": "riverhog_client.transform.CollectionTransformRuntime",
  "unit": "member"
}
```
