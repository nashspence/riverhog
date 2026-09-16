# stove0_operator_contracts.AdmissionIntent.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionintent-seal:c6cda2a198 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-48086caac5"></a>
- <a id="s-72d5ead971"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-00ea73714e"></a>`module`: `stove0_operator_contracts`
- <a id="s-f4a53bc88f"></a>`name`: `seal`
- <a id="s-803678106a"></a>`owner`: `stove0_operator_contracts.AdmissionIntent`
- <a id="s-00042b6eeb"></a>`unit`: `member`

### Declared structure

- <a id="s-edb2ede046"></a>`kind`: `"classmethod"`
- <a id="s-089656d4a3"></a>`signature`: `"\"(cls, *, policy: 'AdmissionPolicy', collection: 'CatalogSyncDescriptor') -> 'AdmissionIntent'\""`

## Maintained corroboration

### Related interface records

- [AdmissionIntent](stove0-operator-contracts-admissionintent.md)

## Governing policies

- <a id="pa-3b598a1607"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionIntent.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73ebae9f37494358e1629a07e4785ccf4518367cf76ff6f4c6201a4c864db7fc -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, policy: 'AdmissionPolicy', collection: 'CatalogSyncDescriptor') -> 'AdmissionIntent'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "seal",
  "owner": "stove0_operator_contracts.AdmissionIntent",
  "unit": "member"
}
```

</details>
