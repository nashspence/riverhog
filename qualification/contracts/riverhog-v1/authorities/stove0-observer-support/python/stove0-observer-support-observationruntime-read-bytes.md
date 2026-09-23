# stove0_observer_support.ObservationRuntime.read_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationruntim-21ce2db19d:c76c3a66c6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6aa9d9c88b"></a>
- <a id="s-60a7f7c886"></a>`distribution`: `stove0-observer-support`
- <a id="s-9efc75cea0"></a>`module`: `stove0_observer_support`
- <a id="s-c146473310"></a>`name`: `read_bytes`
- <a id="s-8861b4faa9"></a>`owner`: `stove0_observer_support.ObservationRuntime`
- <a id="s-b6a444e564"></a>`unit`: `member`

### Declared structure

- <a id="s-4e3cde45d6"></a>`kind`: `"method"`
- <a id="s-27fedbdc58"></a>`signature`: `"\"(self, subject: 'ArtifactSubject', *, maximum_bytes: 'int', **prepare_kwargs: 'Any') -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [ObservationRuntime](stove0-observer-support-observationruntime.md)

## Governing policies

- <a id="pa-a1b2cb4f9b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationRuntime.read_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 58172033bc9893bc6db0e1269461e1b18775dc59162fc1469aa8daa3cf1b1593 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, subject: 'ArtifactSubject', *, maximum_bytes: 'int', **prepare_kwargs: 'Any') -> 'bytes'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "read_bytes",
  "owner": "stove0_observer_support.ObservationRuntime",
  "unit": "member"
}
```

</details>
