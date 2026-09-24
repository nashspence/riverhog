# lifecycle_events.lifecycle_event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-lifecycle-event:6af8979fcd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-421e4a032c"></a>
- <a id="s-554515535b"></a>`distribution`: `lifecycle-events`
- <a id="s-b1e49dd4dd"></a>`module`: `lifecycle_events`
- <a id="s-beac30ec25"></a>`name`: `lifecycle_event`
- <a id="s-5734131e0a"></a>`unit`: `export`

### Declared structure

- <a id="s-4470c2dd59"></a>`kind`: `"function"`
- <a id="s-4f48248ba1"></a>`signature`: `"\"(*, type: 'str', payload: 'Mapping[str, Any] \| None' = None, subject: 'str \| None' = None, occurred_at: 'datetime \| None' = None, event_id: 'str \| None' = None) -> 'LifecycleEvent'\""`

## Governing policies

- <a id="pa-350dac5311"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources/authorities.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.lifecycle_event`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55fa58c29e47df0c8d0282ca983c0870011e71a1c7719f72dfa3e5f6a932a84b -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, type: 'str', payload: 'Mapping[str, Any] | None' = None, subject: 'str | None' = None, occurred_at: 'datetime | None' = None, event_id: 'str | None' = None) -> 'LifecycleEvent'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "lifecycle_event",
  "unit": "export"
}
```

</details>
