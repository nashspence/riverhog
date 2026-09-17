# stove0_core.Stove0RiverhogClient.target_authority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-target-authority:a7faab6f6e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6ac7a72e50"></a>
- <a id="s-6f9b390bd9"></a>`distribution`: `stove0-server`
- <a id="s-6c4fb93c0d"></a>`module`: `stove0_core`
- <a id="s-95d20cf537"></a>`name`: `target_authority`
- <a id="s-5dc331b9bf"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-f301076a72"></a>`unit`: `member`

### Declared structure

- <a id="s-32e1d1baea"></a>`kind`: `"method"`
- <a id="s-773bbdf500"></a>`signature`: `"\"(self, claim: 'ClaimBinding', evidence: 'ControllerEvidence', target_plan: 'TargetPlan', inputs: 'Iterable[ArtifactSubject]') -> 'TargetInvocationAuthority'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-cb5d7865bf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.target_authority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98791913986235de669a7050b1060d6539625509d7bd132b87020acf6b7db803 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim: 'ClaimBinding', evidence: 'ControllerEvidence', target_plan: 'TargetPlan', inputs: 'Iterable[ArtifactSubject]') -> 'TargetInvocationAuthority'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_authority",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```

</details>
