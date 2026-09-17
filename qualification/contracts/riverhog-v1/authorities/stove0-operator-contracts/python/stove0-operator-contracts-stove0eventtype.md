# stove0_operator_contracts.Stove0EventType

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0eventtype:cb3616d686 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d4a37eb063"></a>
- <a id="s-0b80831788"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-714aad2c43"></a>`module`: `stove0_operator_contracts`
- <a id="s-76a69825e0"></a>`name`: `Stove0EventType`
- <a id="s-e23a09cb4a"></a>`unit`: `export`

### Declared structure

- <a id="s-1dfe0fb152"></a>`kind`: `"type-alias"`
- <a id="s-46a1fff529"></a>`value`: `"typing.Literal['io.riverhog.stove0.work.created', 'io.riverhog.stove0.work.updated', 'io.riverhog.stove0.branch-set.admitted', 'io.riverhog.stove0.join.admitted', 'io.riverhog.stove0.evaluation.created', 'io.riverhog.stove0.evaluation.updated']"`

## Governing policies

- <a id="pa-71a1bbfd34"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0EventType`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d2ca5383ed69dee5060b06984ab7c10e454752597cef23e5f2398ab363d2e3b9 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['io.riverhog.stove0.work.created', 'io.riverhog.stove0.work.updated', 'io.riverhog.stove0.branch-set.admitted', 'io.riverhog.stove0.join.admitted', 'io.riverhog.stove0.evaluation.created', 'io.riverhog.stove0.evaluation.updated']"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "Stove0EventType",
  "unit": "export"
}
```

</details>
