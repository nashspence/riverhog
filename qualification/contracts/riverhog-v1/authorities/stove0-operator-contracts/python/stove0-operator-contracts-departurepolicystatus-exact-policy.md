# stove0_operator_contracts.DeparturePolicyStatus.exact_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departurepolicy-e26ef67bec:1e353fb27c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c50a60f7d5"></a>
- <a id="s-47e5ed66c1"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-5a82f9aa8f"></a>`module`: `stove0_operator_contracts`
- <a id="s-6c7b373e2c"></a>`name`: `exact_policy`
- <a id="s-d82188e0c8"></a>`owner`: `stove0_operator_contracts.DeparturePolicyStatus`
- <a id="s-979fef183f"></a>`unit`: `member`

### Declared structure

- <a id="s-ef3c77891f"></a>`kind`: `"method"`
- <a id="s-00db275d00"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [DeparturePolicyStatus](stove0-operator-contracts-departurepolicystatus.md)

## Governing policies

- <a id="pa-5f5e4fb4f4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DeparturePolicyStatus.exact_policy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c365a58bf480d0f4841f0903216652883047050346688207126f792d7f77937 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "exact_policy",
  "owner": "stove0_operator_contracts.DeparturePolicyStatus",
  "unit": "member"
}
```

</details>
