# stove0_operator_contracts.WorkView.from_record

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workview-from-record:6858cb56bb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7c401361c9"></a>
- <a id="s-52237d0f61"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-6928d01611"></a>`module`: `stove0_operator_contracts`
- <a id="s-75c3f42f28"></a>`name`: `from_record`
- <a id="s-0a82da66c4"></a>`owner`: `stove0_operator_contracts.WorkView`
- <a id="s-6450dac131"></a>`unit`: `member`

### Declared structure

- <a id="s-ed35c3f2fc"></a>`kind`: `"classmethod"`
- <a id="s-b6f904f855"></a>`signature`: `"\"(cls, record: 'BaseModel \| Mapping[str, Any]') -> 'WorkView'\""`

## Maintained corroboration

### Related interface records

- [WorkView](stove0-operator-contracts-workview.md)

## Governing policies

- <a id="pa-34d998c722"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkView.from_record`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 06ac3dd7c177d2114845d7ce318f208e72f0c5da9087ecb3270a4db41770bb70 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, record: 'BaseModel | Mapping[str, Any]') -> 'WorkView'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "from_record",
  "owner": "stove0_operator_contracts.WorkView",
  "unit": "member"
}
```

</details>
