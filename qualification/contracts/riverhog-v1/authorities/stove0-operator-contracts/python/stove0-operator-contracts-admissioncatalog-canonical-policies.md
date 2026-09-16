# stove0_operator_contracts.AdmissionCatalog.canonical_policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissioncatalo-f4174cb4f4:ccf0d0338c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1350d8dae1"></a>
- <a id="s-8c868ab062"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-4e0785031d"></a>`module`: `stove0_operator_contracts`
- <a id="s-bfa03d462d"></a>`name`: `canonical_policies`
- <a id="s-c710c1bf13"></a>`owner`: `stove0_operator_contracts.AdmissionCatalog`
- <a id="s-160d7b6852"></a>`unit`: `member`

### Declared structure

- <a id="s-cd89c07dc0"></a>`kind`: `"classmethod"`
- <a id="s-79aa11baae"></a>`signature`: `"\"(cls, value: 'tuple[AdmissionPolicy, ...]') -> 'tuple[AdmissionPolicy, ...]'\""`

## Maintained corroboration

### Related interface records

- [AdmissionCatalog](stove0-operator-contracts-admissioncatalog.md)

## Governing policies

- <a id="pa-3713501780"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionCatalog.canonical_policies`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a47a0a76a67faf4c75174fa5349eae03494ab58511424eb73fae467f32831ff2 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[AdmissionPolicy, ...]') -> 'tuple[AdmissionPolicy, ...]'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "canonical_policies",
  "owner": "stove0_operator_contracts.AdmissionCatalog",
  "unit": "member"
}
```

</details>
