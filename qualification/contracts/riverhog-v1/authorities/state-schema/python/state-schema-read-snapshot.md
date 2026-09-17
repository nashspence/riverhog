# state_schema.read_snapshot

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-read-snapshot:4340d0ce8c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-85046865fc"></a>
- <a id="s-423e0f8e15"></a>`distribution`: `state-schema`
- <a id="s-1647b5d865"></a>`module`: `state_schema`
- <a id="s-5374a10fb8"></a>`name`: `read_snapshot`
- <a id="s-da040639a2"></a>`unit`: `export`

### Declared structure

- <a id="s-324fa0634d"></a>`kind`: `"function"`
- <a id="s-ab5051712a"></a>`signature`: `"\"(session_factory: 'SessionFactory') -> 'CollectionsIterator[Session]'\""`

## Governing policies

- <a id="pa-6f7cc5961d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.read_snapshot`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4123a0cee4bb4a179c916f89103d040f836b62655d97a05253e78b4968c2dcaa -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(session_factory: 'SessionFactory') -> 'CollectionsIterator[Session]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "read_snapshot",
  "unit": "export"
}
```

</details>
