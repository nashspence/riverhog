# stove0_operator_contracts.Stove0Event.exact_subject

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0event-exact-subject:b963e8081b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e1377a5c86"></a>
- <a id="s-686cc6919a"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-e96047482d"></a>`module`: `stove0_operator_contracts`
- <a id="s-8361fa4488"></a>`name`: `exact_subject`
- <a id="s-f957fa6234"></a>`owner`: `stove0_operator_contracts.Stove0Event`
- <a id="s-599e54650f"></a>`unit`: `member`

### Declared structure

- <a id="s-19345d29b6"></a>`kind`: `"method"`
- <a id="s-9ac0e57c20"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [Stove0Event](stove0-operator-contracts-stove0event.md)

## Governing policies

- <a id="pa-5aea9ad512"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0Event.exact_subject`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c8f0896b0aec70fd2d149043ee634577c1497380bd236897ad8c8b92e53f1e20 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "exact_subject",
  "owner": "stove0_operator_contracts.Stove0Event",
  "unit": "member"
}
```

</details>
