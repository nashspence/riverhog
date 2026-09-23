# stove0_observer_support.ContentObservationRuntime.subjects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-1151d7a3b2:3dfd7ac6b2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d54a0c285"></a>
- <a id="s-72c90b1ed2"></a>`distribution`: `stove0-observer-support`
- <a id="s-761ec40304"></a>`module`: `stove0_observer_support`
- <a id="s-60e3e8d389"></a>`name`: `subjects`
- <a id="s-23311c5bca"></a>`owner`: `stove0_observer_support.ContentObservationRuntime`
- <a id="s-fb408d10b5"></a>`unit`: `member`

### Declared structure

- <a id="s-b3b2416ed1"></a>`kind`: `"method"`
- <a id="s-ac82222439"></a>`signature`: `"\"(self) -> 'tuple[tuple[ArtifactSubject, ClaimedArtifact], ...]'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRuntime](stove0-observer-support-contentobservationruntime.md)

## Governing policies

- <a id="pa-8d73033cd4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationRuntime.subjects`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1dddca0e660cee32d277bcfb0888add900839f650fda8e9c7408c5100515bc68 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'tuple[tuple[ArtifactSubject, ClaimedArtifact], ...]'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "subjects",
  "owner": "stove0_observer_support.ContentObservationRuntime",
  "unit": "member"
}
```

</details>
