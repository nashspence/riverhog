# lifecycle_events.SQLiteLifecycleEventLog.expire_context

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-sqlitelifecycleeventlog-a23354b515:732523362e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-72f3ed05f2"></a>
- <a id="s-ab2802b4b7"></a>`distribution`: `lifecycle-events`
- <a id="s-abaa89d161"></a>`module`: `lifecycle_events`
- <a id="s-8382d70e72"></a>`name`: `expire_context`
- <a id="s-12adc1405f"></a>`owner`: `lifecycle_events.SQLiteLifecycleEventLog`
- <a id="s-0979b34905"></a>`unit`: `member`

### Declared structure

- <a id="s-a2520ee446"></a>`kind`: `"method"`
- <a id="s-21b5e7c673"></a>`signature`: `"\"(self, *, owner: 'str', subject: 'str', expires_at: 'str') -> 'int'\""`

## Maintained corroboration

### Related interface records

- [SQLiteLifecycleEventLog](lifecycle-events-sqlitelifecycleeventlog.md)

## Governing policies

- <a id="pa-ad8f7705b1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.SQLiteLifecycleEventLog.expire_context`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5dd48663f212a585cb2457212089c0c2edae9950c688f017383f176b1b2a2ec9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, owner: 'str', subject: 'str', expires_at: 'str') -> 'int'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "expire_context",
  "owner": "lifecycle_events.SQLiteLifecycleEventLog",
  "unit": "member"
}
```
