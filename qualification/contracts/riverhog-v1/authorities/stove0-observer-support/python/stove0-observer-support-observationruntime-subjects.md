# stove0_observer_support.ObservationRuntime.subjects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationruntime-subjects:c61440a8ac -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-80dd02aa8b"></a>
- <a id="s-64eb8d1db1"></a>`distribution`: `stove0-observer-support`
- <a id="s-8fd664b8f3"></a>`module`: `stove0_observer_support`
- <a id="s-f9a88d033d"></a>`name`: `subjects`
- <a id="s-7f2aa511b8"></a>`owner`: `stove0_observer_support.ObservationRuntime`
- <a id="s-da3b08aca7"></a>`unit`: `member`

### Declared structure

- <a id="s-b3d32d2ad4"></a>`kind`: `"method"`
- <a id="s-4a6cfd5c84"></a>`signature`: `"\"(self) -> 'tuple[tuple[ArtifactSubject, ClaimedArtifact], ...]'\""`

## Maintained corroboration

### Related interface records

- [ObservationRuntime](stove0-observer-support-observationruntime.md)

## Governing policies

- <a id="pa-b012cfcf05"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationRuntime.subjects`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 236fbf9ee5fd311a41bace96514ca0d48b9cb8210d168b02fba2cc7b2d5b3954 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'tuple[tuple[ArtifactSubject, ClaimedArtifact], ...]'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "subjects",
  "owner": "stove0_observer_support.ObservationRuntime",
  "unit": "member"
}
```

</details>
