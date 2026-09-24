# stove0_operator_contracts.WorkCreateRequest.canonical_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatereque-47b7db66aa:c095c9dd27 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7db5fe8525"></a>
- <a id="s-94cc35aa3a"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-2997e35b0f"></a>`module`: `stove0_operator_contracts`
- <a id="s-f0e0609d55"></a>`name`: `canonical_inputs`
- <a id="s-781a0b6a8c"></a>`owner`: `stove0_operator_contracts.WorkCreateRequest`
- <a id="s-a39da423c3"></a>`unit`: `member`

### Declared structure

- <a id="s-e6a7d586c0"></a>`kind`: `"classmethod"`
- <a id="s-0d3222468f"></a>`signature`: `"\"(cls, value: 'tuple[CollectionRootIdentityRef, ...]') -> 'tuple[CollectionRootIdentityRef, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkCreateRequest](stove0-operator-contracts-workcreaterequest.md)

## Governing policies

- <a id="pa-7193deb083"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreateRequest.canonical_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 946c14395afc75065996032015fcae65d1e3ac055a970bbd3ed29f8c02b66481 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[CollectionRootIdentityRef, ...]') -> 'tuple[CollectionRootIdentityRef, ...]'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "canonical_inputs",
  "owner": "stove0_operator_contracts.WorkCreateRequest",
  "unit": "member"
}
```

</details>
