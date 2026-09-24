# stove0_operator_contracts.WorkCreateIn.canonical_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatein-ca-2d209281a3:5f0f9b6e9b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a45ad46e32"></a>
- <a id="s-44afc8754f"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-27732f09a2"></a>`module`: `stove0_operator_contracts`
- <a id="s-39b16a1ae4"></a>`name`: `canonical_inputs`
- <a id="s-016b7a6ec9"></a>`owner`: `stove0_operator_contracts.WorkCreateIn`
- <a id="s-13f2555a8a"></a>`unit`: `member`

### Declared structure

- <a id="s-59ac20633a"></a>`kind`: `"classmethod"`
- <a id="s-81cf21b1c4"></a>`signature`: `"\"(cls, value: 'tuple[CollectionRootIdentityRef, ...]') -> 'tuple[CollectionRootIdentityRef, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkCreateIn](stove0-operator-contracts-workcreatein.md)

## Governing policies

- <a id="pa-ba87393d9b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreateIn.canonical_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63342f77abc7daf3ecd61fec4366c91b5976d9e77f20476aae35f93d6511da86 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[CollectionRootIdentityRef, ...]') -> 'tuple[CollectionRootIdentityRef, ...]'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "canonical_inputs",
  "owner": "stove0_operator_contracts.WorkCreateIn",
  "unit": "member"
}
```

</details>
