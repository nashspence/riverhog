# stove0_observer_support.ObservationRuntime.open_workspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationruntim-8fd45dda15:db5c5b5ea7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b507a58f3"></a>
- <a id="s-72ed919688"></a>`distribution`: `stove0-observer-support`
- <a id="s-8a03d47450"></a>`module`: `stove0_observer_support`
- <a id="s-dd19fdd683"></a>`name`: `open_workspace`
- <a id="s-c72530d4b4"></a>`owner`: `stove0_observer_support.ObservationRuntime`
- <a id="s-79f4049f22"></a>`unit`: `member`

### Declared structure

- <a id="s-a87a34dba1"></a>`kind`: `"method"`
- <a id="s-dc233aac75"></a>`signature`: `"\"(self, root: 'Path') -> 'TransformWorkspace'\""`

## Maintained corroboration

### Related interface records

- [stove0_observer_support.ObservationRuntime](stove0-observer-support-observationruntime.md)

## Governing policies

- <a id="pa-3ace5464f0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationRuntime.open_workspace`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc45b1a2c96f159cc3fe80f0922a9eb740329a11e21da1ebb3bea5613492d5a0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, root: 'Path') -> 'TransformWorkspace'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "open_workspace",
  "owner": "stove0_observer_support.ObservationRuntime",
  "unit": "member"
}
```
