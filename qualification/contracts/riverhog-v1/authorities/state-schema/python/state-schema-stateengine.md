# state_schema.StateEngine

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine:b4d2fe8dda -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bb9d308d87"></a>
| Field | Shape |
|---|---|
| <a id="s-43e1c0af09"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-6f676b2f90"></a>`distribution` | "state-schema" |
| <a id="s-1e17c11185"></a>`module` | "state_schema" |
| <a id="s-d6e84b5cae"></a>`name` | "StateEngine" |
| <a id="s-6862ffd2d3"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [state_schema.StateEngine.begin](state-schema-stateengine-begin.md)
- [state_schema.StateEngine.clear_compiled_cache](state-schema-stateengine-clear-compiled-cache.md)
- [state_schema.StateEngine.connect](state-schema-stateengine-connect.md)
- [state_schema.StateEngine.dispose](state-schema-stateengine-dispose.md)
- [state_schema.StateEngine.driver](state-schema-stateengine-driver.md)
- [state_schema.StateEngine.engine](state-schema-stateengine-engine.md)
- [state_schema.StateEngine.execution_options](state-schema-stateengine-execution-options.md)
- [state_schema.StateEngine.get_execution_options](state-schema-stateengine-get-execution-options.md)
- [state_schema.StateEngine.name](state-schema-stateengine-name.md)
- [state_schema.StateEngine.raw_connection](state-schema-stateengine-raw-connection.md)
- [state_schema.StateEngine.update_execution_options](state-schema-stateengine-update-execution-options.md)

## Governing policies

- <a id="pa-6759f53a1d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateEngine`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1634227fc68b11263720025853345414f1da7c412acea82dfc5d7b03c8ba1638 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(pool: 'Pool', dialect: 'Dialect', url: 'URL', logging_name: 'Optional[str]' = None, echo: 'Optional[_EchoFlagType]' = None, query_cache_size: 'int' = 500, execution_options: 'Optional[Mapping[str, Any]]' = None, hide_parameters: 'bool' = False)\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "StateEngine",
  "unit": "export"
}
```
