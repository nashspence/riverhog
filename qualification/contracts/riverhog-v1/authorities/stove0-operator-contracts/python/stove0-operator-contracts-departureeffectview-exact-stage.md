# stove0_operator_contracts.DepartureEffectView.exact_stage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departureeffect-64763e79ac:cf4dd8e414 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d6baeec08c"></a>
- <a id="s-a146f0c116"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-58e3d1b287"></a>`module`: `stove0_operator_contracts`
- <a id="s-7ec9d3ffb3"></a>`name`: `exact_stage`
- <a id="s-4b4e2f0829"></a>`owner`: `stove0_operator_contracts.DepartureEffectView`
- <a id="s-9b661a9034"></a>`unit`: `member`

### Declared structure

- <a id="s-82cc780ba8"></a>`kind`: `"method"`
- <a id="s-b3e1871a52"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectView](stove0-operator-contracts-departureeffectview.md)

## Governing policies

- <a id="pa-6cb389525b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DepartureEffectView.exact_stage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dad993670807faaeaaabb90b4ee49cfa7513646b43a725f875efe48b0886e3a0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "exact_stage",
  "owner": "stove0_operator_contracts.DepartureEffectView",
  "unit": "member"
}
```

</details>
