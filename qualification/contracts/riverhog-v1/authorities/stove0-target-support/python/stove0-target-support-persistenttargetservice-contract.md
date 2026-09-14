# stove0_target_support.PersistentTargetService.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-persistenttargetser-a0a2a83409:9489d4ca9c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a75d0a8062"></a>
- <a id="s-df1149c8e7"></a>`distribution`: `stove0-target-support`
- <a id="s-6e15d754a2"></a>`module`: `stove0_target_support`
- <a id="s-a4920ba1fa"></a>`name`: `contract`
- <a id="s-8a8568b917"></a>`owner`: `stove0_target_support.PersistentTargetService`
- <a id="s-de4ca23415"></a>`unit`: `member`

### Declared structure

- <a id="s-b42ed1b2b6"></a>`kind`: `"method"`
- <a id="s-c751b76d64"></a>`signature`: `"\"(self) -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.PersistentTargetService](stove0-target-support-persistenttargetservice.md)

## Governing policies

- <a id="pa-353da36bb9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.PersistentTargetService.contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 296709de33f87a482c356105606eb052ae075e39b374e99f5af088e4038e8de5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "contract",
  "owner": "stove0_target_support.PersistentTargetService",
  "unit": "member"
}
```
