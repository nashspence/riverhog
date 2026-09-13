# stove0_operator_contracts.AdmissionPolicyStatus.exact_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpolicy-995397d618:67275718c4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-368ff80541"></a>
| Field | Shape |
|---|---|
| <a id="s-a4285ba396"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a3d714ee63"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-e02638cd93"></a>`module` | "stove0_operator_contracts" |
| <a id="s-aea04a4b3b"></a>`name` | "exact_policy" |
| <a id="s-253f62dc98"></a>`owner` | "stove0_operator_contracts.AdmissionPolicyStatus" |
| <a id="s-adf2869f8a"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.AdmissionPolicyStatus](stove0-operator-contracts-admissionpolicystatus.md)

## Governing policies

- <a id="pa-83f06ba06b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPolicyStatus.exact_policy`

### Exact owned JSON

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
