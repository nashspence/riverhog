# riverhog_client.processing.ClaimedCollectionRuntime.prepare_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-4a6901e2ff:f156d8b327 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6abdf9dc55"></a>
- <a id="s-6a3a2a14b7"></a>`distribution`: `riverhog-client`
- <a id="s-dd5f90b47f"></a>`module`: `riverhog_client.processing`
- <a id="s-c6911b1f6b"></a>`name`: `prepare_inputs`
- <a id="s-f5bb8d4f63"></a>`owner`: `riverhog_client.processing.ClaimedCollectionRuntime`
- <a id="s-94196fd97e"></a>`unit`: `member`

### Declared structure

- <a id="s-4e77b1a6ed"></a>`kind`: `"method"`
- <a id="s-03a4051821"></a>`signature`: `"\"(self, artifacts: 'Sequence[ClaimedArtifact] \| None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntime](riverhog-client-processing-claimedcollectionruntime.md)

## Governing policies

- <a id="pa-0f0f1dee2d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionRuntime.prepare_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 881d06cabb35aab9a8db711e64927d8e9ad16e59eda7c3bde5f217b96dee1656 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifacts: 'Sequence[ClaimedArtifact] | None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "prepare_inputs",
  "owner": "riverhog_client.processing.ClaimedCollectionRuntime",
  "unit": "member"
}
```

</details>
