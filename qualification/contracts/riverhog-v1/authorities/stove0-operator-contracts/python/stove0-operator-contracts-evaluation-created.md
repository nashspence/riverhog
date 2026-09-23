# stove0_operator_contracts.EVALUATION_CREATED

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluation-created:8436f4c76c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-65e6a64327"></a>
- <a id="s-0a553be2b7"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-e3d6ac28a0"></a>`module`: `stove0_operator_contracts`
- <a id="s-482b7244c6"></a>`name`: `EVALUATION_CREATED`
- <a id="s-bf6a3d9f80"></a>`unit`: `export`

### Declared structure

- <a id="s-43189cade2"></a>`kind`: `"constant"`
- <a id="s-6edeea1876"></a>`value`: `"io.riverhog.stove0.evaluation.created"`

## Governing policies

- <a id="pa-cbbb3b8b90"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EVALUATION_CREATED`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89e9d71b0dbf86149539d249d8bceeb03d7f8b269ecd93b1c801d89e785a0859 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "io.riverhog.stove0.evaluation.created"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EVALUATION_CREATED",
  "unit": "export"
}
```

</details>
