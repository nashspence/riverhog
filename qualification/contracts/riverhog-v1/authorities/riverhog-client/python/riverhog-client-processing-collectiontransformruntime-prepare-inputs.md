# riverhog_client.processing.CollectionTransformRuntime.prepare_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-collectiontran-9d5ec5b059:e61a9b3017 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1bc383131a"></a>
- <a id="s-a8b6ab5c44"></a>`distribution`: `riverhog-client`
- <a id="s-71f5c73639"></a>`module`: `riverhog_client.processing`
- <a id="s-d5f7811acd"></a>`name`: `prepare_inputs`
- <a id="s-ded6fa4133"></a>`owner`: `riverhog_client.processing.CollectionTransformRuntime`
- <a id="s-5919d6e49f"></a>`unit`: `member`

### Declared structure

- <a id="s-30a0ee1165"></a>`kind`: `"method"`
- <a id="s-a282260274"></a>`signature`: `"\"(self, artifacts: 'Sequence[ClaimedArtifact] \| None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-processing-collectiontransformruntime.md)

## Governing policies

- <a id="pa-ec8fa61b88"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CollectionTransformRuntime.prepare_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8ee865c68ffe636bb6481bdd6d85d274d9362de9c91840098485ae3f29d99f3b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifacts: 'Sequence[ClaimedArtifact] | None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "prepare_inputs",
  "owner": "riverhog_client.processing.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
