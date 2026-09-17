# stove0_protocol.WorkflowPreviewPayload.canonical_selections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewpayload-ca-728c5a0475:8b041b317e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-39698bf164"></a>
- <a id="s-8f3305c178"></a>`distribution`: `stove0-protocol`
- <a id="s-85476edda4"></a>`module`: `stove0_protocol`
- <a id="s-6672ed6e2f"></a>`name`: `canonical_selections`
- <a id="s-8f5fc2e654"></a>`owner`: `stove0_protocol.WorkflowPreviewPayload`
- <a id="s-cb501f5c04"></a>`unit`: `member`

### Declared structure

- <a id="s-c456ea9b93"></a>`kind`: `"classmethod"`
- <a id="s-a952b26541"></a>`signature`: `"\"(cls, value: 'tuple[ArtifactSelection, ...]') -> 'tuple[ArtifactSelection, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreviewPayload](stove0-protocol-workflowpreviewpayload.md)

## Governing policies

- <a id="pa-2933f65295"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewPayload.canonical_selections`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7fe61473e61560aa25a7cc02e4e839c96b076289fcc065075b94ea17bca878a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ArtifactSelection, ...]') -> 'tuple[ArtifactSelection, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_selections",
  "owner": "stove0_protocol.WorkflowPreviewPayload",
  "unit": "member"
}
```

</details>
