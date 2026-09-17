# state_schema.StateSchema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateschema:9552236842 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d73474f03b"></a>
- <a id="s-7fe7cac73f"></a>`distribution`: `state-schema`
- <a id="s-0dae4ef676"></a>`module`: `state_schema`
- <a id="s-a6d45282d8"></a>`name`: `StateSchema`
- <a id="s-b59b871120"></a>`unit`: `export`

### Declared structure

- <a id="s-f61ed91b3c"></a>`kind`: `"class"`
- <a id="s-fd8982e026"></a>`signature`: `"\"(*, name: 'str', engine_factory: 'EngineFactory', script_location: 'Path', verify: 'SchemaVerify', prerequisite: 'SchemaVerify \| None' = None, is_empty: 'EmptyStateCheck \| None' = None, version_table: 'str' = 'state_schema_revision') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [status](state-schema-stateschema-status.md)
- [upgrade_connection](state-schema-stateschema-upgrade-connection.md)
- [upgrade](state-schema-stateschema-upgrade.md)
- [validate](state-schema-stateschema-validate.md)

## Governing policies

- <a id="pa-0857355274"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateSchema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cdea8c7dc750614e018635aec988cfa029aaf9f1e3432c09574ee9f2cbc1eee7 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, name: 'str', engine_factory: 'EngineFactory', script_location: 'Path', verify: 'SchemaVerify', prerequisite: 'SchemaVerify | None' = None, is_empty: 'EmptyStateCheck | None' = None, version_table: 'str' = 'state_schema_revision') -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "StateSchema",
  "unit": "export"
}
```

</details>
