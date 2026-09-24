# stove0_target_support.DepartureEffectTargetService.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-departureeffecttarg-8299cac916:c361912ea7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-88e305d517"></a>
- <a id="s-8b6e5e020f"></a>`distribution`: `stove0-target-support`
- <a id="s-85bb1500c0"></a>`module`: `stove0_target_support`
- <a id="s-3950b358bd"></a>`name`: `descriptor`
- <a id="s-49e650458c"></a>`owner`: `stove0_target_support.DepartureEffectTargetService`
- <a id="s-4c2329191f"></a>`unit`: `member`

### Declared structure

- <a id="s-0198a630c9"></a>`kind`: `"method"`
- <a id="s-346cb25205"></a>`signature`: `"\"(self) -> 'DepartureEffectTargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectTargetService](stove0-target-support-departureeffecttargetservice.md)

## Governing policies

- <a id="pa-b232d60038"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.DepartureEffectTargetService.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d77d1cf3a22e199d211e45e7efbf1e8ade9d087f1ba84412aa87350a266edb6b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'DepartureEffectTargetDescriptor'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "descriptor",
  "owner": "stove0_target_support.DepartureEffectTargetService",
  "unit": "member"
}
```

</details>
