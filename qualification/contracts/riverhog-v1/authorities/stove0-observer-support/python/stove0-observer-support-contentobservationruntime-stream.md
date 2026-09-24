# stove0_observer_support.ContentObservationRuntime.stream

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-91ae59ac04:4766cd6819 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-25a1c429c3"></a>
- <a id="s-f34018c50f"></a>`distribution`: `stove0-observer-support`
- <a id="s-abde7484c7"></a>`module`: `stove0_observer_support`
- <a id="s-67411f0695"></a>`name`: `stream`
- <a id="s-24aa287170"></a>`owner`: `stove0_observer_support.ContentObservationRuntime`
- <a id="s-2e332788e1"></a>`unit`: `member`

### Declared structure

- <a id="s-e0ace78a2a"></a>`kind`: `"method"`
- <a id="s-3ed90ff2c3"></a>`signature`: `"\"(self, subject: 'WorkArtifactSubject', *, start: 'int' = 0, end: 'int \| None' = None, chunk_size: 'int' = 8388608, **prepare_kwargs: 'Any') -> 'Iterator[Iterator[bytes]]'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRuntime](stove0-observer-support-contentobservationruntime.md)

## Governing policies

- <a id="pa-9efa6b7f0a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationRuntime.stream`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8278746b0d45791462addd55dec3c0dafa6f780830bfcbf466d14f18e7debba -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, subject: 'WorkArtifactSubject', *, start: 'int' = 0, end: 'int | None' = None, chunk_size: 'int' = 8388608, **prepare_kwargs: 'Any') -> 'Iterator[Iterator[bytes]]'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "stream",
  "owner": "stove0_observer_support.ContentObservationRuntime",
  "unit": "member"
}
```

</details>
