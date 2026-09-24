# stove0_target_support.DepartureEffectTargetService.put_departure_effect

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-departureeffecttarg-4b98233eee:acce9b452a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a07e4f56c8"></a>
- <a id="s-5563d10eee"></a>`distribution`: `stove0-target-support`
- <a id="s-c118593d17"></a>`module`: `stove0_target_support`
- <a id="s-5bd44d540d"></a>`name`: `put_departure_effect`
- <a id="s-c3a24aca02"></a>`owner`: `stove0_target_support.DepartureEffectTargetService`
- <a id="s-eabcff6244"></a>`unit`: `member`

### Declared structure

- <a id="s-e70baa5ee0"></a>`kind`: `"method"`
- <a id="s-97564257ac"></a>`signature`: `"\"(self, intent: 'DepartureEffectIntent') -> 'DepartureEffectReceipt'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectTargetService](stove0-target-support-departureeffecttargetservice.md)

## Governing policies

- <a id="pa-9fbc901bd2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.DepartureEffectTargetService.put_departure_effect`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe0e35aace91ccae85879fa5bcef12c2a5adff12bc09268cc08c441727e9c63a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, intent: 'DepartureEffectIntent') -> 'DepartureEffectReceipt'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "put_departure_effect",
  "owner": "stove0_target_support.DepartureEffectTargetService",
  "unit": "member"
}
```

</details>
