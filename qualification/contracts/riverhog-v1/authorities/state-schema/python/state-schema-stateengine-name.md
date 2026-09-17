# state_schema.StateEngine.name

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine-name:99cd40491c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0edbf2d3f2"></a>
- <a id="s-56241463a8"></a>`distribution`: `state-schema`
- <a id="s-f705f6c3a4"></a>`module`: `state_schema`
- <a id="s-a007bf4527"></a>`name`: `name`
- <a id="s-c4770bb1f6"></a>`owner`: `state_schema.StateEngine`
- <a id="s-bac60b1872"></a>`unit`: `member`

### Declared structure

- <a id="s-2aa95f148b"></a>`kind`: `"property"`
- <a id="s-6a6d204d8e"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [StateEngine](state-schema-stateengine.md)

## Governing policies

- <a id="pa-25ac60bdea"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateEngine.name`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d904a6c1bfe3e337b5f74e591f4a07c5f178feb6b42c8c9dab87d7318b34169 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "name",
  "owner": "state_schema.StateEngine",
  "unit": "member"
}
```

</details>
