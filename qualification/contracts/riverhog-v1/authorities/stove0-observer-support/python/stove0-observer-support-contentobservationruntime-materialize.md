# stove0_observer_support.ContentObservationRuntime.materialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-9f275e9bed:3a60c5582c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bcb47cc5e8"></a>
- <a id="s-0fec652d69"></a>`distribution`: `stove0-observer-support`
- <a id="s-8702100b9d"></a>`module`: `stove0_observer_support`
- <a id="s-a2b40f27fb"></a>`name`: `materialize`
- <a id="s-97f784f137"></a>`owner`: `stove0_observer_support.ContentObservationRuntime`
- <a id="s-cbda94d00c"></a>`unit`: `member`

### Declared structure

- <a id="s-320b7ba2c1"></a>`kind`: `"method"`
- <a id="s-d212dcb7fe"></a>`signature`: `"\"(self, subject: 'ArtifactSubject', *, workspace: 'ProcessingWorkspace', relative_path: 'str \| None' = None, **prepare_kwargs: 'Any') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRuntime](stove0-observer-support-contentobservationruntime.md)

## Governing policies

- <a id="pa-022f04cc1c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationRuntime.materialize`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0888ff14e2a58f4d2cde88745d428a544d133de13bf5ac27e431c42eb48fed8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, subject: 'ArtifactSubject', *, workspace: 'ProcessingWorkspace', relative_path: 'str | None' = None, **prepare_kwargs: 'Any') -> 'Path'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "materialize",
  "owner": "stove0_observer_support.ContentObservationRuntime",
  "unit": "member"
}
```

</details>
