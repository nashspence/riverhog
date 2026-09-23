# stove0_operator_contracts.ADMISSION_POLICY_COUNT_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admission-policy-count-max:ad3d1c0ca6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c5cd267500"></a>
- <a id="s-fb0d1c0291"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-e09ac10c4d"></a>`module`: `stove0_operator_contracts`
- <a id="s-b24f8a760a"></a>`name`: `ADMISSION_POLICY_COUNT_MAX`
- <a id="s-96c0d6c14d"></a>`unit`: `export`

### Declared structure

- <a id="s-4d269b061e"></a>`kind`: `"constant"`
- <a id="s-0f5af25989"></a>`value`: `100`

## Governing policies

- <a id="pa-beebf70b5c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.ADMISSION_POLICY_COUNT_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c489d7a7fa723589ff656f8fd061d6a0ae0c6b27cb22b4e785af82099a082167 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 100
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "ADMISSION_POLICY_COUNT_MAX",
  "unit": "export"
}
```

</details>
