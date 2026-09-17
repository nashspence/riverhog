# state_schema.StateConnection.execute

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-execute:8ae75d83c1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ad852eba8f"></a>
- <a id="s-f01964fa70"></a>`distribution`: `state-schema`
- <a id="s-73f82f45c6"></a>`module`: `state_schema`
- <a id="s-5ecff73c4b"></a>`name`: `execute`
- <a id="s-acc34594fa"></a>`owner`: `state_schema.StateConnection`
- <a id="s-4f04a14e3c"></a>`unit`: `member`

### Declared structure

- <a id="s-eaa05d5311"></a>`kind`: `"method"`
- <a id="s-9282fcf3e3"></a>`signature`: `"\"(self, statement: 'Executable', parameters: 'Optional[_CoreAnyExecuteParams]' = None, *, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'CursorResult[Any]'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-9a79df8ee6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateConnection.execute`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a516b596035a4c397fa985b7afa2b4b16d99c240fb8ae353d47882af3e5c75e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, statement: 'Executable', parameters: 'Optional[_CoreAnyExecuteParams]' = None, *, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'CursorResult[Any]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "execute",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
