# stove0_observer_support.ObservationRuntime.stream

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationruntime-stream:da2e7b4a17 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5e67e97701"></a>
- <a id="s-9a97ed5d0b"></a>`distribution`: `stove0-observer-support`
- <a id="s-6f847b17ed"></a>`module`: `stove0_observer_support`
- <a id="s-eb7cae3ace"></a>`name`: `stream`
- <a id="s-4dd2efea0f"></a>`owner`: `stove0_observer_support.ObservationRuntime`
- <a id="s-ea86811f6b"></a>`unit`: `member`

### Declared structure

- <a id="s-03ec3cbda2"></a>`kind`: `"method"`
- <a id="s-625f05c8f3"></a>`signature`: `"\"(self, subject: 'ArtifactSubject', *, start: 'int' = 0, end: 'int \| None' = None, chunk_size: 'int' = 8388608, **prepare_kwargs: 'Any') -> 'Iterator[Iterator[bytes]]'\""`

## Maintained corroboration

### Related interface records

- [ObservationRuntime](stove0-observer-support-observationruntime.md)

## Governing policies

- <a id="pa-0a45e2e294"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationRuntime.stream`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0023dea4e22fb05dff8316c54122a204847de22417852772225ef05289e02a35 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, subject: 'ArtifactSubject', *, start: 'int' = 0, end: 'int | None' = None, chunk_size: 'int' = 8388608, **prepare_kwargs: 'Any') -> 'Iterator[Iterator[bytes]]'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "stream",
  "owner": "stove0_observer_support.ObservationRuntime",
  "unit": "member"
}
```

</details>
