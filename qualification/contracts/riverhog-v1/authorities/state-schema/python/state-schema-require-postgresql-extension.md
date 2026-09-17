# state_schema.require_postgresql_extension

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-require-postgresql-extension:8d2128507c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ba794c0229"></a>
- <a id="s-c813d9e7aa"></a>`distribution`: `state-schema`
- <a id="s-b55335f8ab"></a>`module`: `state_schema`
- <a id="s-8b52125a03"></a>`name`: `require_postgresql_extension`
- <a id="s-6c269ed802"></a>`unit`: `export`

### Declared structure

- <a id="s-fbf580bc84"></a>`kind`: `"function"`
- <a id="s-0af86d9d8b"></a>`signature`: `"\"(connection: 'Connection', *, name: 'str', schema: 'str', accepted_versions: 'tuple[str, ...]' = (), operator_classes: 'tuple[str, ...]' = ()) -> 'None'\""`

## Governing policies

- <a id="pa-3b514fe077"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.require_postgresql_extension`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e58edc643a6bd76d38b27c302df773322eb78df6035ffda7322c1236c7685daa -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(connection: 'Connection', *, name: 'str', schema: 'str', accepted_versions: 'tuple[str, ...]' = (), operator_classes: 'tuple[str, ...]' = ()) -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "require_postgresql_extension",
  "unit": "export"
}
```

</details>
