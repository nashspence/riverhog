# stove0_operator_contracts.WorkCreatedEvent.validate_time

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatedeven-cce371fa05:2a780216d7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d19041f3a4"></a>
- <a id="s-066416ae53"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-183f064ea1"></a>`module`: `stove0_operator_contracts`
- <a id="s-22a9dc4130"></a>`name`: `validate_time`
- <a id="s-94b8ba3320"></a>`owner`: `stove0_operator_contracts.WorkCreatedEvent`
- <a id="s-4ba26963e5"></a>`unit`: `member`

### Declared structure

- <a id="s-9dc7042d54"></a>`kind`: `"classmethod"`
- <a id="s-926697f621"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [WorkCreatedEvent](stove0-operator-contracts-workcreatedevent.md)

## Governing policies

- <a id="pa-981c62fd45"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreatedEvent.validate_time`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a4b548313ce1bea4cabb4adc8726463e57181fa53308e2830dab688aa154f0e4 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "validate_time",
  "owner": "stove0_operator_contracts.WorkCreatedEvent",
  "unit": "member"
}
```

</details>
