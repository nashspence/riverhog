# stove0_operator_contracts.AdmissionPolicyStatus.exact_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpolicy-995397d618:67275718c4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-368ff80541"></a>
- <a id="s-a3d714ee63"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-e02638cd93"></a>`module`: `stove0_operator_contracts`
- <a id="s-aea04a4b3b"></a>`name`: `exact_policy`
- <a id="s-253f62dc98"></a>`owner`: `stove0_operator_contracts.AdmissionPolicyStatus`
- <a id="s-adf2869f8a"></a>`unit`: `member`

### Declared structure

- <a id="s-b0740bd7eb"></a>`kind`: `"method"`
- <a id="s-a051dce374"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [AdmissionPolicyStatus](stove0-operator-contracts-admissionpolicystatus.md)

## Governing policies

- <a id="pa-83f06ba06b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPolicyStatus.exact_policy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6edea08257d6a89d100adaffc6130e6771aa3d1a2b373393aa3976ff666d111d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "exact_policy",
  "owner": "stove0_operator_contracts.AdmissionPolicyStatus",
  "unit": "member"
}
```

</details>
