# state_schema.StateConnection.schema_for_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-schema-for-object:17e94271ad -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f5c0e0d993"></a>
| Field | Shape |
|---|---|
| <a id="s-8138c7ae8b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c20de89593"></a>`distribution` | "state-schema" |
| <a id="s-48a38676f0"></a>`module` | "state_schema" |
| <a id="s-9a7f5c7922"></a>`name` | "schema_for_object" |
| <a id="s-8fd1b08482"></a>`owner` | "state_schema.StateConnection" |
| <a id="s-d545ca2c4d"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-4cf702cb7d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.schema_for_object`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0df9e8d4c7cf9b4cc2c4a78d6dbb2cff3458e2e617046c5aeffadb5aaba6323 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, obj: 'HasSchemaAttr') -> 'Optional[str]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "schema_for_object",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
