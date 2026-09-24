# stove0_target_support.DepartureEffectHttpBinding.handle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-departureeffecthttp-c5b8af9313:8eabb131c5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-512b421496"></a>
- <a id="s-0fcd7a984e"></a>`distribution`: `stove0-target-support`
- <a id="s-a62fbb2dd7"></a>`module`: `stove0_target_support`
- <a id="s-07bd0de740"></a>`name`: `handle`
- <a id="s-0b8fbfad64"></a>`owner`: `stove0_target_support.DepartureEffectHttpBinding`
- <a id="s-8c1a24dd20"></a>`unit`: `member`

### Declared structure

- <a id="s-bd755cd110"></a>`kind`: `"method"`
- <a id="s-1a9000dbcb"></a>`signature`: `"\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'TargetHttpResponse'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectHttpBinding](stove0-target-support-departureeffecthttpbinding.md)

## Governing policies

- <a id="pa-40572f13a1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.DepartureEffectHttpBinding.handle`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f0cfd587894d9bd54f1cfbefb311092a945472d21202fdd48b37c170a567282 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'TargetHttpResponse'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "handle",
  "owner": "stove0_target_support.DepartureEffectHttpBinding",
  "unit": "member"
}
```

</details>
