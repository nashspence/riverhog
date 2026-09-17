# state_schema.StateSchema.validate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateschema-validate:87b3e19ee8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c6d0ffdaef"></a>
- <a id="s-aab6e0172d"></a>`distribution`: `state-schema`
- <a id="s-9069299ed7"></a>`module`: `state_schema`
- <a id="s-24fc04b39d"></a>`name`: `validate`
- <a id="s-455ef108fc"></a>`owner`: `state_schema.StateSchema`
- <a id="s-2237e0b5ad"></a>`unit`: `member`

### Declared structure

- <a id="s-76af9f30fb"></a>`kind`: `"method"`
- <a id="s-81a9785900"></a>`signature`: `"\"(self) -> 'StateStatus'\""`

## Maintained corroboration

### Related interface records

- [StateSchema](state-schema-stateschema.md)

## Governing policies

- <a id="pa-c875d79736"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateSchema.validate`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eb81d4cf9201c27d82c122b49d8587aa90a08cb47593a6f621d31d5a07725271 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'StateStatus'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "validate",
  "owner": "state_schema.StateSchema",
  "unit": "member"
}
```

</details>
