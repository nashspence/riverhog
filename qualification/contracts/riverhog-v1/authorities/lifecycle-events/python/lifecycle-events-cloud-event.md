# lifecycle_events.cloud_event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-cloud-event:b8f2d915fb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f518d4f97c"></a>
- <a id="s-9d0f283465"></a>`distribution`: `lifecycle-events`
- <a id="s-912abb750e"></a>`module`: `lifecycle_events`
- <a id="s-99c6ca2071"></a>`name`: `cloud_event`
- <a id="s-c11f16be09"></a>`unit`: `export`

### Declared structure

- <a id="s-589bf5075c"></a>`kind`: `"function"`
- <a id="s-e73cea745e"></a>`signature`: `"\"(*, source: 'str', type: 'str', data: 'Mapping[str, Any] \| None' = None, subject: 'str \| None' = None, occurred_at: 'datetime \| None' = None, event_id: 'str \| None' = None) -> 'CloudEvent'\""`

## Governing policies

- <a id="pa-618652e11f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.cloud_event`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e371835b3bc18c9839d0bda2a3ce2d1266ea73b73f3e9de772c9f5d9e03216a9 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, source: 'str', type: 'str', data: 'Mapping[str, Any] | None' = None, subject: 'str | None' = None, occurred_at: 'datetime | None' = None, event_id: 'str | None' = None) -> 'CloudEvent'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "cloud_event",
  "unit": "export"
}
```
