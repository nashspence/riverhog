# stove0_observer_support.ContentObservationRuntime.heartbeat

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-1127977b0d:682bc29d39 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b0079d32aa"></a>
- <a id="s-eceaf85489"></a>`distribution`: `stove0-observer-support`
- <a id="s-1707243fc8"></a>`module`: `stove0_observer_support`
- <a id="s-d264df6824"></a>`name`: `heartbeat`
- <a id="s-cb3eab2967"></a>`owner`: `stove0_observer_support.ContentObservationRuntime`
- <a id="s-d2cf052a5a"></a>`unit`: `member`

### Declared structure

- <a id="s-6f5e2f6577"></a>`kind`: `"method"`
- <a id="s-6b646c3329"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRuntime](stove0-observer-support-contentobservationruntime.md)

## Governing policies

- <a id="pa-947a08cc7f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationRuntime.heartbeat`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a88388cb9de0d55c9f14cbd36a5e1f470a8cdd18718426c03fb3f0cffc2ad09 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "heartbeat",
  "owner": "stove0_observer_support.ContentObservationRuntime",
  "unit": "member"
}
```

</details>
