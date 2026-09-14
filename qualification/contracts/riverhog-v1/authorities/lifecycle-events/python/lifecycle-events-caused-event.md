# lifecycle_events.caused_event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-caused-event:225af500fc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6308e5a753"></a>
- <a id="s-1f00657537"></a>`distribution`: `lifecycle-events`
- <a id="s-53f55b94ae"></a>`module`: `lifecycle_events`
- <a id="s-0d821e6ba8"></a>`name`: `caused_event`
- <a id="s-d953c03274"></a>`unit`: `export`

### Declared structure

- <a id="s-4a6561acfb"></a>`kind`: `"function"`
- <a id="s-e5854aee9d"></a>`signature`: `"\"(*, cause: 'CloudEvent', source: 'str', type: 'str', data: 'Mapping[str, Any] \| None' = None, subject: 'str \| None' = None, occurred_at: 'datetime \| None' = None) -> 'CloudEvent'\""`

## Governing policies

- <a id="pa-2f50b16928"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.caused_event`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d6e7d68a82685424a6650cd38095decffdd774926306481c14f14edaa5e3cc0 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, cause: 'CloudEvent', source: 'str', type: 'str', data: 'Mapping[str, Any] | None' = None, subject: 'str | None' = None, occurred_at: 'datetime | None' = None) -> 'CloudEvent'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "caused_event",
  "unit": "export"
}
```
