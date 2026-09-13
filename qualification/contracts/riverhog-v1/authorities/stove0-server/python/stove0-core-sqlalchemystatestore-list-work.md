# stove0_core.SqlAlchemyStateStore.list_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-list-work:01ea9bce08 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d5208bc61d"></a>
| Field | Shape |
|---|---|
| <a id="s-d676f8eedc"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-75bb39f7f0"></a>`distribution` | "stove0-server" |
| <a id="s-d7c6d61359"></a>`module` | "stove0_core" |
| <a id="s-1cec50a8bd"></a>`name` | "list_work" |
| <a id="s-e53c3ad605"></a>`owner` | "stove0_core.SqlAlchemyStateStore" |
| <a id="s-7aa8e3fcaa"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-cf7ff2a4e4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.list_work`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39c6c76611c2eb3ac8a9f0a53f9c3d428aab83c44bb12f656975c4b15bff99ef -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, position: 'tuple[str | int | bool | bytes | None, ...] | None' = None, phase: 'str | None' = None, query: 'str | None' = None, sort: 'WorkSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'dict[str, object]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "list_work",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
