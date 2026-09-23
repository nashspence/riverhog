# stove0_observer_support.ContentObserver.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobserver-observe:eb0589eae8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-54cf3880fe"></a>
- <a id="s-1c7fcd9831"></a>`distribution`: `stove0-observer-support`
- <a id="s-57d1be697b"></a>`module`: `stove0_observer_support`
- <a id="s-1ba141abc3"></a>`name`: `observe`
- <a id="s-90544f19dc"></a>`owner`: `stove0_observer_support.ContentObserver`
- <a id="s-ba4f0d6943"></a>`unit`: `member`

### Declared structure

- <a id="s-571c901731"></a>`kind`: `"method"`
- <a id="s-52326eafd7"></a>`signature`: `"\"(self, request: 'ContentObservationRequest', runtime: 'ContentObservationRuntime') -> 'ContentObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ContentObserver](stove0-observer-support-contentobserver.md)

## Governing policies

- <a id="pa-1835c2ce7d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObserver.observe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0429b00c312bc8e91ab591efad23bd0bac95cd330848b2b9c0e2a2ece62da04 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ContentObservationRequest', runtime: 'ContentObservationRuntime') -> 'ContentObservationResult'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "observe",
  "owner": "stove0_observer_support.ContentObserver",
  "unit": "member"
}
```

</details>
