# stove0_observer_support.ContentObserver.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobserver-descriptor:cebcff6221 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c1cbb34d64"></a>
- <a id="s-26a99465db"></a>`distribution`: `stove0-observer-support`
- <a id="s-bd8aef4d47"></a>`module`: `stove0_observer_support`
- <a id="s-2d535947d9"></a>`name`: `descriptor`
- <a id="s-c739f6590d"></a>`owner`: `stove0_observer_support.ContentObserver`
- <a id="s-ceb965fdba"></a>`unit`: `member`

### Declared structure

- <a id="s-22c24105a8"></a>`kind`: `"method"`
- <a id="s-8c98f7701d"></a>`signature`: `"\"(self) -> 'ObserverDescriptor'\""`

## Maintained corroboration

### Related interface records

- [ContentObserver](stove0-observer-support-contentobserver.md)

## Governing policies

- <a id="pa-e7679edcf3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObserver.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a62413a0a387f3bade82ddc9ae644b00db1332c170d437be0d7120254d6e7302 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ObserverDescriptor'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "descriptor",
  "owner": "stove0_observer_support.ContentObserver",
  "unit": "member"
}
```

</details>
