# stove0_core.Stove0Coordinator

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0coordinator:44b1dcec63 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1430137ed4"></a>
- <a id="s-a6e0647e80"></a>`distribution`: `stove0-server`
- <a id="s-3bac176451"></a>`module`: `stove0_core`
- <a id="s-c437567062"></a>`name`: `Stove0Coordinator`
- <a id="s-979f7880cf"></a>`unit`: `export`

### Declared structure

- <a id="s-e944aa968f"></a>`kind`: `"class"`
- <a id="s-4407ce5137"></a>`signature`: `"\"(work: 'Stove0WorkService', *, riverhog: 'RiverhogControlPort', planning: 'PlanningPort', observers: 'ObserverPort', targets: 'TargetPort', target_callbacks: 'TargetCallbackPort') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [cancel](stove0-core-stove0coordinator-cancel.md)
- [create_or_resume](stove0-core-stove0coordinator-create-or-resume.md)
- [inspect_coordination](stove0-core-stove0coordinator-inspect-coordination.md)
- [retry](stove0-core-stove0coordinator-retry.md)
- [step](stove0-core-stove0coordinator-step.md)

## Governing policies

- <a id="pa-0022bd27e6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0Coordinator`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 20b56fd0b55716b8c481552afffb646b57a515a7f9809bbcd9350f50f96a6600 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(work: 'Stove0WorkService', *, riverhog: 'RiverhogControlPort', planning: 'PlanningPort', observers: 'ObserverPort', targets: 'TargetPort', target_callbacks: 'TargetCallbackPort') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "Stove0Coordinator",
  "unit": "export"
}
```

</details>
