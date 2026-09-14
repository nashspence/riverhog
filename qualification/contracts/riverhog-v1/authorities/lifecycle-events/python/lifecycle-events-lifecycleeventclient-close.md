# lifecycle_events.LifecycleEventClient.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-lifecycleeventclient-close:8b1c4cbc11 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8eaa7eebd9"></a>
- <a id="s-7d3acfb447"></a>`distribution`: `lifecycle-events`
- <a id="s-f103f68f58"></a>`module`: `lifecycle_events`
- <a id="s-20aa89c119"></a>`name`: `close`
- <a id="s-c07e919235"></a>`owner`: `lifecycle_events.LifecycleEventClient`
- <a id="s-00c95b91fe"></a>`unit`: `member`

### Declared structure

- <a id="s-1aa34ceac2"></a>`kind`: `"method"`
- <a id="s-3991a8ab1c"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [LifecycleEventClient](lifecycle-events-lifecycleeventclient.md)

## Governing policies

- <a id="pa-a47deb849e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.LifecycleEventClient.close`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 48e4d89280e97e62d9ca02d91ce671a3d6bc026eadfc984ffa938b74e68c1551 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "close",
  "owner": "lifecycle_events.LifecycleEventClient",
  "unit": "member"
}
```
