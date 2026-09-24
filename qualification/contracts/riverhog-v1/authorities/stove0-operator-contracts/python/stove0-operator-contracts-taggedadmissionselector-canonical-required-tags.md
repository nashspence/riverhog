# stove0_operator_contracts.TaggedAdmissionSelector.canonical_required_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-taggedadmission-01840c0fc5:78599bb616 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fababb1afa"></a>
- <a id="s-a0db0048e0"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-659afb5acd"></a>`module`: `stove0_operator_contracts`
- <a id="s-bf7e76c212"></a>`name`: `canonical_required_tags`
- <a id="s-bb11c372c2"></a>`owner`: `stove0_operator_contracts.TaggedAdmissionSelector`
- <a id="s-8768cb2762"></a>`unit`: `member`

### Declared structure

- <a id="s-bfc9b4d0d3"></a>`kind`: `"classmethod"`
- <a id="s-2dc3ded7b7"></a>`signature`: `"\"(cls, value: 'tuple[CollectionTag, ...]') -> 'tuple[CollectionTag, ...]'\""`

## Maintained corroboration

### Related interface records

- [TaggedAdmissionSelector](stove0-operator-contracts-taggedadmissionselector.md)

## Governing policies

- <a id="pa-17c0e4ee7a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.TaggedAdmissionSelector.canonical_required_tags`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 51856942b65d61095429e9bb2be48fb70edc089361ed8a4397d50ccad2e7e78d -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[CollectionTag, ...]') -> 'tuple[CollectionTag, ...]'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "canonical_required_tags",
  "owner": "stove0_operator_contracts.TaggedAdmissionSelector",
  "unit": "member"
}
```

</details>
