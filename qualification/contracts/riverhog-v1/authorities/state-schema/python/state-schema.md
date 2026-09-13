# state_schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema:feff2c5bf8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1c4972e32e"></a>
| Field | Shape |
|---|---|
| <a id="s-1be5c38592"></a>`candidate_id` | "python:state-schema:state_schema" |
| <a id="s-5307ecce02"></a>`distribution` | "state-schema" |
| <a id="s-85636c0a67"></a>`exports` | additional keys=`StateCondition`, `StateConnection`, `StateEngine`, `StateSchema`, `StateSchemaError`, `StateStatus`, `assert_schema_matches_metadata`, `attach_sha256_string_constraints`, `read_snapshot`, `require_postgresql_extension`, `run_migration_environment`, `sqlite_engine` |
| <a id="s-9531552189"></a>`module` | "state_schema" |

## Governing policies

- <a id="pa-7e36e979db"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/31`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75492dcc3549115feca51aa512927bf914a1b7e18ec24d62a1c5f0d9265cba89 -->

```json
{
  "candidate_id": "python:state-schema:state_schema",
  "distribution": "state-schema",
  "exports": {
    "StateCondition": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "StateConnection": {
      "kind": "class",
      "members": {
        "begin": {
          "kind": "method",
          "signature": "\"(self) -> 'RootTransaction'\""
        },
        "begin_nested": {
          "kind": "method",
          "signature": "\"(self) -> 'NestedTransaction'\""
        },
        "begin_twophase": {
          "kind": "method",
          "signature": "\"(self, xid: 'Optional[Any]' = None) -> 'TwoPhaseTransaction'\""
        },
        "close": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
        },
        "closed": {
          "kind": "property",
          "signature": "\"(self) -> 'bool'\""
        },
        "commit": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
        },
        "commit_prepared": {
          "kind": "method",
          "signature": "\"(self, xid: 'Any', recover: 'bool' = False) -> 'None'\""
        },
        "connection": {
          "kind": "property",
          "signature": "\"(self) -> 'PoolProxiedConnection'\""
        },
        "default_isolation_level": {
          "kind": "property",
          "signature": "\"(self) -> 'Optional[IsolationLevel]'\""
        },
        "detach": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
        },
        "exec_driver_sql": {
          "kind": "method",
          "signature": "\"(self, statement: 'str', parameters: 'Optional[_DBAPIAnyExecuteParams]' = None, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'CursorResult[Any]'\""
        },
        "execute": {
          "kind": "method",
          "signature": "\"(self, statement: 'Executable', parameters: 'Optional[_CoreAnyExecuteParams]' = None, *, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'CursorResult[Any]'\""
        },
        "execution_options": {
          "kind": "method",
          "signature": "\"(self, **opt: 'Any') -> 'Connection'\""
        },
        "get_execution_options": {
          "kind": "method",
          "signature": "\"(self) -> '_ExecuteOptions'\""
        },
        "get_isolation_level": {
          "kind": "method",
          "signature": "\"(self) -> 'IsolationLevel'\""
        },
        "get_nested_transaction": {
          "kind": "method",
          "signature": "\"(self) -> 'Optional[NestedTransaction]'\""
        },
        "get_transaction": {
          "kind": "method",
          "signature": "\"(self) -> 'Optional[RootTransaction]'\""
        },
        "in_nested_transaction": {
          "kind": "method",
          "signature": "\"(self) -> 'bool'\""
        },
        "in_transaction": {
          "kind": "method",
          "signature": "\"(self) -> 'bool'\""
        },
        "info": {
          "kind": "property",
          "signature": "\"(self) -> '_InfoType'\""
        },
        "invalidate": {
          "kind": "method",
          "signature": "\"(self, exception: 'Optional[BaseException]' = None) -> 'None'\""
        },
        "invalidated": {
          "kind": "property",
          "signature": "\"(self) -> 'bool'\""
        },
        "recover_twophase": {
          "kind": "method",
          "signature": "\"(self) -> 'List[Any]'\""
        },
        "rollback": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
        },
        "rollback_prepared": {
          "kind": "method",
          "signature": "\"(self, xid: 'Any', recover: 'bool' = False) -> 'None'\""
        },
        "scalar": {
          "kind": "method",
          "signature": "\"(self, statement: 'Executable', parameters: 'Optional[_CoreSingleExecuteParams]' = None, *, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'Any'\""
        },
        "scalars": {
          "kind": "method",
          "signature": "\"(self, statement: 'Executable', parameters: 'Optional[_CoreAnyExecuteParams]' = None, *, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'ScalarResult[Any]'\""
        },
        "schema_for_object": {
          "kind": "method",
          "signature": "\"(self, obj: 'HasSchemaAttr') -> 'Optional[str]'\""
        }
      },
      "signature": "\"(engine: 'Engine', connection: 'Optional[PoolProxiedConnection]' = None, _has_events: 'Optional[bool]' = None, _allow_revalidate: 'bool' = True, _allow_autobegin: 'bool' = True)\""
    },
    "StateEngine": {
      "kind": "class",
      "members": {
        "begin": {
          "kind": "method",
          "signature": "\"(self) -> 'Iterator[Connection]'\""
        },
        "clear_compiled_cache": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
        },
        "connect": {
          "kind": "method",
          "signature": "\"(self) -> 'Connection'\""
        },
        "dispose": {
          "kind": "method",
          "signature": "\"(self, close: 'bool' = True) -> 'None'\""
        },
        "driver": {
          "kind": "property",
          "signature": "\"(self) -> 'str'\""
        },
        "engine": {
          "kind": "property",
          "signature": "\"(self) -> 'Engine'\""
        },
        "execution_options": {
          "kind": "method",
          "signature": "\"(self, **opt: 'Any') -> 'OptionEngine'\""
        },
        "get_execution_options": {
          "kind": "method",
          "signature": "\"(self) -> '_ExecuteOptions'\""
        },
        "name": {
          "kind": "property",
          "signature": "\"(self) -> 'str'\""
        },
        "raw_connection": {
          "kind": "method",
          "signature": "\"(self) -> 'PoolProxiedConnection'\""
        },
        "update_execution_options": {
          "kind": "method",
          "signature": "\"(self, **opt: 'Any') -> 'None'\""
        }
      },
      "signature": "\"(pool: 'Pool', dialect: 'Dialect', url: 'URL', logging_name: 'Optional[str]' = None, echo: 'Optional[_EchoFlagType]' = None, query_cache_size: 'int' = 500, execution_options: 'Optional[Mapping[str, Any]]' = None, hide_parameters: 'bool' = False)\""
    },
    "StateSchema": {
      "kind": "class",
      "members": {
        "status": {
          "kind": "method",
          "signature": "\"(self) -> 'StateStatus'\""
        },
        "upgrade": {
          "kind": "method",
          "signature": "\"(self) -> 'StateStatus'\""
        },
        "upgrade_connection": {
          "kind": "method",
          "signature": "\"(self, connection: 'Connection') -> 'StateStatus'\""
        },
        "validate": {
          "kind": "method",
          "signature": "\"(self) -> 'StateStatus'\""
        }
      },
      "signature": "\"(*, name: 'str', engine_factory: 'EngineFactory', script_location: 'Path', verify: 'SchemaVerify', prerequisite: 'SchemaVerify | None' = None, is_empty: 'EmptyStateCheck | None' = None, version_table: 'str' = 'state_schema_revision') -> 'None'\""
    },
    "StateSchemaError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "StateStatus": {
      "fields": [
        {
          "default": "required",
          "name": "name",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "condition",
          "type": "'StateCondition'"
        },
        {
          "default": "required",
          "name": "current_revision",
          "type": "'str | None'"
        },
        {
          "default": "required",
          "name": "head_revision",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "as_dict": {
          "kind": "method",
          "signature": "\"(self) -> 'dict[str, str | None]'\""
        }
      },
      "signature": "\"(name: 'str', condition: 'StateCondition', current_revision: 'str | None', head_revision: 'str') -> None\""
    },
    "assert_schema_matches_metadata": {
      "kind": "function",
      "signature": "\"(bind: 'Connection | Engine', metadata: 'MetaData', *, version_table: 'str') -> 'None'\""
    },
    "attach_sha256_string_constraints": {
      "kind": "function",
      "signature": "\"(metadata: 'MetaData') -> 'None'\""
    },
    "read_snapshot": {
      "kind": "function",
      "signature": "\"(session_factory: 'SessionFactory') -> 'CollectionsIterator[Session]'\""
    },
    "require_postgresql_extension": {
      "kind": "function",
      "signature": "\"(connection: 'Connection', *, name: 'str', schema: 'str', accepted_versions: 'tuple[str, ...]' = (), operator_classes: 'tuple[str, ...]' = ()) -> 'None'\""
    },
    "run_migration_environment": {
      "kind": "function",
      "signature": "\"() -> 'None'\""
    },
    "sqlite_engine": {
      "kind": "function",
      "signature": "\"(path: 'Path') -> 'Engine'\""
    }
  },
  "module": "state_schema"
}
```
