# stove0_observer_support.ObservationRuntime.materialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationruntim-f22e19e138:ed12e91b0d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d0ecc7cec8"></a>
- <a id="s-ae8d3a10af"></a>`distribution`: `stove0-observer-support`
- <a id="s-e5a0ddb891"></a>`module`: `stove0_observer_support`
- <a id="s-d96892cc08"></a>`name`: `materialize`
- <a id="s-dc918c9ff1"></a>`owner`: `stove0_observer_support.ObservationRuntime`
- <a id="s-f6c1469258"></a>`unit`: `member`

### Declared structure

- <a id="s-32a12cd6fc"></a>`kind`: `"method"`
- <a id="s-c24f1ead84"></a>`signature`: `"\"(self, subject: 'ArtifactSubject', *, workspace: 'TransformWorkspace', relative_path: 'str \| None' = None, **prepare_kwargs: 'Any') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [ObservationRuntime](stove0-observer-support-observationruntime.md)

## Governing policies

- <a id="pa-87406ef113"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationRuntime.materialize`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a347314a51f345dafe71c78714155ab3d67f08ff26a312995175290ece04610 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, subject: 'ArtifactSubject', *, workspace: 'TransformWorkspace', relative_path: 'str | None' = None, **prepare_kwargs: 'Any') -> 'Path'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "materialize",
  "owner": "stove0_observer_support.ObservationRuntime",
  "unit": "member"
}
```

</details>
