# stove0_operator_contracts.Stove0LifecycleEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0lifecycleevent:3e3a655db5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-26e8908858"></a>
- <a id="s-e0f674201d"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-7a170751af"></a>`module`: `stove0_operator_contracts`
- <a id="s-f9af881038"></a>`name`: `Stove0LifecycleEvent`
- <a id="s-ffb3f70a0f"></a>`unit`: `export`

### Declared structure

- <a id="s-b6a95ee230"></a>`kind`: `"type-alias"`
- <a id="s-b507a5cf05"></a>`value`: `"typing.Annotated[stove0_operator_contracts.WorkCreatedEvent \| stove0_operator_contracts.WorkUpdatedEvent \| stove0_operator_contracts.BranchSetAdmittedEvent \| stove0_operator_contracts.JoinAdmittedEvent \| stove0_operator_contracts.EvaluationCreatedEvent \| stove0_operator_contracts.EvaluationUpdatedEvent, FieldInfo(annotation=NoneType, required=True, discriminator='type')]"`

## Governing policies

- <a id="pa-e8343efdc2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0LifecycleEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84e9bf47bc8d4a902e11d92dbf8247dab4f64a02a08c6d0183efd20c14f6d35a -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[stove0_operator_contracts.WorkCreatedEvent | stove0_operator_contracts.WorkUpdatedEvent | stove0_operator_contracts.BranchSetAdmittedEvent | stove0_operator_contracts.JoinAdmittedEvent | stove0_operator_contracts.EvaluationCreatedEvent | stove0_operator_contracts.EvaluationUpdatedEvent, FieldInfo(annotation=NoneType, required=True, discriminator='type')]"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "Stove0LifecycleEvent",
  "unit": "export"
}
```

</details>
