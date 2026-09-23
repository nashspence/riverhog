# stove0_operator_contracts.AdmissionPolicy.policy_sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpolicy-3f103a16fd:f5970982d5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9e8200ce2a"></a>
- <a id="s-a7d96558b9"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-fed88ec8ff"></a>`module`: `stove0_operator_contracts`
- <a id="s-57d14acf26"></a>`name`: `policy_sha256`
- <a id="s-e72fed68ee"></a>`owner`: `stove0_operator_contracts.AdmissionPolicy`
- <a id="s-eda37a3353"></a>`unit`: `member`

### Declared structure

- <a id="s-3c8d804ae5"></a>`kind`: `"property"`
- <a id="s-4e72ff7f4e"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [AdmissionPolicy](stove0-operator-contracts-admissionpolicy.md)

## Governing policies

- <a id="pa-ab9eafbf54"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPolicy.policy_sha256`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 457bc1498c75d67ade8c974fcec47db5277b2e1e7601b7d2084ec6e2a866fc9a -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "policy_sha256",
  "owner": "stove0_operator_contracts.AdmissionPolicy",
  "unit": "member"
}
```

</details>
