# stove0_operator_contracts.AdmissionPolicy.canonical_required_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpolicy-241e675154:f2530e4a61 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0adce5b997"></a>
- <a id="s-07e6895ea1"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-71d05c80ca"></a>`module`: `stove0_operator_contracts`
- <a id="s-02c4f1fef3"></a>`name`: `canonical_required_tags`
- <a id="s-9e87e17573"></a>`owner`: `stove0_operator_contracts.AdmissionPolicy`
- <a id="s-f7704b576d"></a>`unit`: `member`

### Declared structure

- <a id="s-8002c43775"></a>`kind`: `"classmethod"`
- <a id="s-9031f18e7f"></a>`signature`: `"\"(cls, value: 'tuple[CollectionTag, ...]') -> 'tuple[CollectionTag, ...]'\""`

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.AdmissionPolicy](stove0-operator-contracts-admissionpolicy.md)

## Governing policies

- <a id="pa-b7e25bd7e9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPolicy.canonical_required_tags`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d099298d85eea286be8aee9a723497e7d4fbb2bb0f1cf00fb98b365e128307e -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[CollectionTag, ...]') -> 'tuple[CollectionTag, ...]'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "canonical_required_tags",
  "owner": "stove0_operator_contracts.AdmissionPolicy",
  "unit": "member"
}
```
