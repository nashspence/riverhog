# stove0_operator_contracts.JoinAdmittedEvent.validate_time

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-joinadmittedeve-bbeeae9a43:6c4b376927 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-12bf2a3a53"></a>
- <a id="s-02727b73ac"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-30fb3c773d"></a>`module`: `stove0_operator_contracts`
- <a id="s-ad6f6a0823"></a>`name`: `validate_time`
- <a id="s-639b71560f"></a>`owner`: `stove0_operator_contracts.JoinAdmittedEvent`
- <a id="s-f776c47a82"></a>`unit`: `member`

### Declared structure

- <a id="s-af63c8157e"></a>`kind`: `"classmethod"`
- <a id="s-5e329c11a8"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [JoinAdmittedEvent](stove0-operator-contracts-joinadmittedevent.md)

## Governing policies

- <a id="pa-79b1c7d969"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.JoinAdmittedEvent.validate_time`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 547f8173e8cb0b1b0588dfeea450e11f1dc90c8fc1266f0766526e4ee7b0a926 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "validate_time",
  "owner": "stove0_operator_contracts.JoinAdmittedEvent",
  "unit": "member"
}
```

</details>
