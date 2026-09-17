# lifecycle_events.create_lifecycle_event_schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-create-lifecycle-event-schema:6767075e06 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-489dd664ed"></a>
- <a id="s-ce72326a1f"></a>`distribution`: `lifecycle-events`
- <a id="s-63c50838f0"></a>`module`: `lifecycle_events`
- <a id="s-dc2c5b06a2"></a>`name`: `create_lifecycle_event_schema`
- <a id="s-7520b0418c"></a>`unit`: `export`

### Declared structure

- <a id="s-e8bcc1ea10"></a>`kind`: `"function"`
- <a id="s-fbbe831f75"></a>`signature`: `"\"(connection: 'sqlite3.Connection') -> 'None'\""`

## Governing policies

- <a id="pa-f2929acfd3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources/authorities.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.create_lifecycle_event_schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d71a3347e228ad4b24575a242a24d62d5e16b3965f32f3e5fa82e15cb4018a98 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(connection: 'sqlite3.Connection') -> 'None'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "create_lifecycle_event_schema",
  "unit": "export"
}
```

</details>
