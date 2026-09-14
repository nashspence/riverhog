# lifecycle_events.SQLiteLifecycleEventLog.append

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-sqlitelifecycleeventlog-append:751ea8caf6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc0269984a"></a>
- <a id="s-15c5e22d2f"></a>`distribution`: `lifecycle-events`
- <a id="s-321958d9c8"></a>`module`: `lifecycle_events`
- <a id="s-ff00a09b47"></a>`name`: `append`
- <a id="s-7de3845bcf"></a>`owner`: `lifecycle_events.SQLiteLifecycleEventLog`
- <a id="s-05333dc52c"></a>`unit`: `member`

### Declared structure

- <a id="s-f5e34cd494"></a>`kind`: `"method"`
- <a id="s-d54b5f2170"></a>`signature`: `"\"(self, event: 'CloudEvent', *, owner: 'str', context: 'dict[str, object] \| None' = None, context_expires_at: 'str \| None' = None) -> 'int'\""`

## Maintained corroboration

### Related interface records

- [SQLiteLifecycleEventLog](lifecycle-events-sqlitelifecycleeventlog.md)

## Governing policies

- <a id="pa-641d765b8e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.SQLiteLifecycleEventLog.append`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f8d269ae803e6e94fb7770e3bb7ac453cfba8998bfe9169e7eac8514488d2d6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, event: 'CloudEvent', *, owner: 'str', context: 'dict[str, object] | None' = None, context_expires_at: 'str | None' = None) -> 'int'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "append",
  "owner": "lifecycle_events.SQLiteLifecycleEventLog",
  "unit": "member"
}
```
