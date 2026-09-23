# stove0_observer_support.ContentObservationRuntime.prepare

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-554c97e59e:914216a112 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cdd810104a"></a>
- <a id="s-86cf07d403"></a>`distribution`: `stove0-observer-support`
- <a id="s-1d6a536b36"></a>`module`: `stove0_observer_support`
- <a id="s-26eb4aab41"></a>`name`: `prepare`
- <a id="s-96f0162e8c"></a>`owner`: `stove0_observer_support.ContentObservationRuntime`
- <a id="s-0518db4dba"></a>`unit`: `member`

### Declared structure

- <a id="s-831d90b331"></a>`kind`: `"method"`
- <a id="s-3e43cab858"></a>`signature`: `"\"(self, subjects: 'Sequence[ArtifactSubject] \| None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRuntime](stove0-observer-support-contentobservationruntime.md)

## Governing policies

- <a id="pa-37b908ba84"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationRuntime.prepare`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f0e3cb78c6ab52a5a8c17d3ae14c91c4041f3fef409825c13800746a32557476 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, subjects: 'Sequence[ArtifactSubject] | None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "prepare",
  "owner": "stove0_observer_support.ContentObservationRuntime",
  "unit": "member"
}
```

</details>
