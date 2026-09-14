# state_schema.StateConnection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection:fb5d7345c5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-194353803b"></a>
- <a id="s-283d73246f"></a>`distribution`: `state-schema`
- <a id="s-1c1cec9c34"></a>`module`: `state_schema`
- <a id="s-272f666ff8"></a>`name`: `StateConnection`
- <a id="s-02be498d6d"></a>`unit`: `export`

### Declared structure

- <a id="s-eba6e561b3"></a>`kind`: `"class"`
- <a id="s-feeac4c569"></a>`signature`: `"\"(engine: 'Engine', connection: 'Optional[PoolProxiedConnection]' = None, _has_events: 'Optional[bool]' = None, _allow_revalidate: 'bool' = True, _allow_autobegin: 'bool' = True)\""`

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection.begin_nested](state-schema-stateconnection-begin-nested.md)
- [state_schema.StateConnection.begin_twophase](state-schema-stateconnection-begin-twophase.md)
- [state_schema.StateConnection.begin](state-schema-stateconnection-begin.md)
- [state_schema.StateConnection.close](state-schema-stateconnection-close.md)
- [state_schema.StateConnection.closed](state-schema-stateconnection-closed.md)
- [state_schema.StateConnection.commit_prepared](state-schema-stateconnection-commit-prepared.md)
- [state_schema.StateConnection.commit](state-schema-stateconnection-commit.md)
- [state_schema.StateConnection.connection](state-schema-stateconnection-connection.md)
- [state_schema.StateConnection.default_isolation_level](state-schema-stateconnection-default-isolation-level.md)
- [state_schema.StateConnection.detach](state-schema-stateconnection-detach.md)
- [state_schema.StateConnection.__enter__](state-schema-stateconnection-enter.md)
- [state_schema.StateConnection.exec_driver_sql](state-schema-stateconnection-exec-driver-sql.md)
- [state_schema.StateConnection.execute](state-schema-stateconnection-execute.md)
- [state_schema.StateConnection.execution_options](state-schema-stateconnection-execution-options.md)
- [state_schema.StateConnection.__exit__](state-schema-stateconnection-exit.md)
- [state_schema.StateConnection.get_execution_options](state-schema-stateconnection-get-execution-options.md)
- [state_schema.StateConnection.get_isolation_level](state-schema-stateconnection-get-isolation-level.md)
- [state_schema.StateConnection.get_nested_transaction](state-schema-stateconnection-get-nested-transaction.md)
- [state_schema.StateConnection.get_transaction](state-schema-stateconnection-get-transaction.md)
- [state_schema.StateConnection.in_nested_transaction](state-schema-stateconnection-in-nested-transaction.md)
- [state_schema.StateConnection.in_transaction](state-schema-stateconnection-in-transaction.md)
- [state_schema.StateConnection.info](state-schema-stateconnection-info.md)
- [state_schema.StateConnection.invalidate](state-schema-stateconnection-invalidate.md)
- [state_schema.StateConnection.invalidated](state-schema-stateconnection-invalidated.md)
- [state_schema.StateConnection.recover_twophase](state-schema-stateconnection-recover-twophase.md)
- [state_schema.StateConnection.rollback_prepared](state-schema-stateconnection-rollback-prepared.md)
- [state_schema.StateConnection.rollback](state-schema-stateconnection-rollback.md)
- [state_schema.StateConnection.scalar](state-schema-stateconnection-scalar.md)
- [state_schema.StateConnection.scalars](state-schema-stateconnection-scalars.md)
- [state_schema.StateConnection.schema_for_object](state-schema-stateconnection-schema-for-object.md)

## Governing policies

- <a id="pa-06405e2b70"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6910791a9ac93676d59e24749bbd18e533b9b5a339086ec2ae656ee0fd2a689 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(engine: 'Engine', connection: 'Optional[PoolProxiedConnection]' = None, _has_events: 'Optional[bool]' = None, _allow_revalidate: 'bool' = True, _allow_autobegin: 'bool' = True)\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "StateConnection",
  "unit": "export"
}
```
