# stove0_observer_support.ContentObservationRuntime.open_workspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-c83b91afa0:754d4ef258 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3d6baeb69d"></a>
- <a id="s-1302a10b94"></a>`distribution`: `stove0-observer-support`
- <a id="s-ccabb3d47b"></a>`module`: `stove0_observer_support`
- <a id="s-6af75c923d"></a>`name`: `open_workspace`
- <a id="s-46bb833ff3"></a>`owner`: `stove0_observer_support.ContentObservationRuntime`
- <a id="s-6be2adde37"></a>`unit`: `member`

### Declared structure

- <a id="s-bc2af7d208"></a>`kind`: `"method"`
- <a id="s-e168871659"></a>`signature`: `"\"(self, root: 'Path') -> 'TransformWorkspace'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRuntime](stove0-observer-support-contentobservationruntime.md)

## Governing policies

- <a id="pa-385815f37a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationRuntime.open_workspace`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08ecd0d877e18ebd4570987893a9af5d830f27c060bc652d428aa762f36b6e35 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, root: 'Path') -> 'TransformWorkspace'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "open_workspace",
  "owner": "stove0_observer_support.ContentObservationRuntime",
  "unit": "member"
}
```

</details>
