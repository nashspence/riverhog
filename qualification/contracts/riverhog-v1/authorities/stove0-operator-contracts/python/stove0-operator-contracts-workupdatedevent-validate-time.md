# stove0_operator_contracts.WorkUpdatedEvent.validate_time

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workupdatedeven-a2f84e2c9e:2aa712ae82 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc855c257d"></a>
- <a id="s-f2a30c8c99"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-cbbf4d67cf"></a>`module`: `stove0_operator_contracts`
- <a id="s-025c6e0f5b"></a>`name`: `validate_time`
- <a id="s-dce275b679"></a>`owner`: `stove0_operator_contracts.WorkUpdatedEvent`
- <a id="s-3b10eb5954"></a>`unit`: `member`

### Declared structure

- <a id="s-bce49c1c2c"></a>`kind`: `"classmethod"`
- <a id="s-0a42b59b8c"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [WorkUpdatedEvent](stove0-operator-contracts-workupdatedevent.md)

## Governing policies

- <a id="pa-132d7dc3c2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkUpdatedEvent.validate_time`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba20f7473aa5aa1f023dad5c2dd203d4070a1ef6fb7c817d9ef104f18332417e -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "validate_time",
  "owner": "stove0_operator_contracts.WorkUpdatedEvent",
  "unit": "member"
}
```

</details>
