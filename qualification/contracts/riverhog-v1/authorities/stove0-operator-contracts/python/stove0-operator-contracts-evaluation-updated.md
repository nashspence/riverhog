# stove0_operator_contracts.EVALUATION_UPDATED

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluation-updated:6350285d6a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f2f5e1259c"></a>
- <a id="s-428e29821a"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-0541428251"></a>`module`: `stove0_operator_contracts`
- <a id="s-10f10488b5"></a>`name`: `EVALUATION_UPDATED`
- <a id="s-11fb2cb706"></a>`unit`: `export`

### Declared structure

- <a id="s-953127cc9c"></a>`kind`: `"constant"`
- <a id="s-c86547ae63"></a>`value`: `"io.riverhog.stove0.evaluation.updated"`

## Governing policies

- <a id="pa-628351677c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EVALUATION_UPDATED`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a2a90ce278b00bd968dbe53b868eb0651f1464d8a90669005ca1d68b1c2f13aa -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "io.riverhog.stove0.evaluation.updated"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EVALUATION_UPDATED",
  "unit": "export"
}
```

</details>
