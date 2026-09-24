# stove0_core.RiverhogControlPort.seal_execution

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-seal-execution:0d7af9b6ba -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-71c456b30d"></a>
- <a id="s-24212369c7"></a>`distribution`: `stove0-server`
- <a id="s-3c231ca196"></a>`module`: `stove0_core`
- <a id="s-fcc3b6b795"></a>`name`: `seal_execution`
- <a id="s-73669aafdd"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-4fbc757e5e"></a>`unit`: `member`

### Declared structure

- <a id="s-b3de006956"></a>`kind`: `"method"`
- <a id="s-2bb579d3eb"></a>`signature`: `"\"(self, claim: 'ClaimBinding', evidence: 'ControllerEvidence', plan: 'WorkflowPlan', target_plan: 'TargetPlan', inputs: 'Iterable[WorkArtifactSubject]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-6b85cc91eb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.seal_execution`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ebe1d5cf0d06875ad1802e9f95cde91160371d895943945c42ac3fe01f67f18 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim: 'ClaimBinding', evidence: 'ControllerEvidence', plan: 'WorkflowPlan', target_plan: 'TargetPlan', inputs: 'Iterable[WorkArtifactSubject]') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "seal_execution",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```

</details>
