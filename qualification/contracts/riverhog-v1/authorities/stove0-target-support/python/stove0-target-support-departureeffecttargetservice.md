# stove0_target_support.DepartureEffectTargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-departureeffecttargetservice:e9e6fe5c52 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-915e7b936e"></a>
- <a id="s-8a2d30c691"></a>`distribution`: `stove0-target-support`
- <a id="s-0a5aeb165c"></a>`module`: `stove0_target_support`
- <a id="s-b092860c1a"></a>`name`: `DepartureEffectTargetService`
- <a id="s-bbf093ec7c"></a>`unit`: `export`

### Declared structure

- <a id="s-8fd8867e56"></a>`kind`: `"class"`
- <a id="s-ba71603d8b"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [put_departure_effect](stove0-target-support-departureeffecttargetservice-put-departure-effect.md)
- [descriptor](stove0-target-support-departureeffecttargetservice-descriptor.md)

## Governing policies

- <a id="pa-65f3544c42"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.DepartureEffectTargetService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6de60847cc5a77e55eec41d35f7ba065040cf4e7cdb8a29c606a381e24521ef4 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "DepartureEffectTargetService",
  "unit": "export"
}
```

</details>
