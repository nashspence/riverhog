# lifecycle_events.LifecycleEventClient.__enter__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-lifecycleeventclient-enter:0ad7b5d94e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b59ece2a7"></a>
- <a id="s-e2a5e3b054"></a>`distribution`: `lifecycle-events`
- <a id="s-f86953edf2"></a>`module`: `lifecycle_events`
- <a id="s-af2d2ae192"></a>`name`: `__enter__`
- <a id="s-6bb64f50bc"></a>`owner`: `lifecycle_events.LifecycleEventClient`
- <a id="s-eae6b11783"></a>`unit`: `member`

### Declared structure

- <a id="s-2049cba14e"></a>`kind`: `"method"`
- <a id="s-9fb3b47b54"></a>`signature`: `"\"(self) -> 'LifecycleEventClient'\""`

## Maintained corroboration

### Related interface records

- [LifecycleEventClient](lifecycle-events-lifecycleeventclient.md)

## Governing policies

- <a id="pa-5c7dc61701"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources/authorities.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.LifecycleEventClient.__enter__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f948629a635d5f10c652319a8bd51da71c12387c9d7ce046dad101ea5a0dd1f3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'LifecycleEventClient'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "__enter__",
  "owner": "lifecycle_events.LifecycleEventClient",
  "unit": "member"
}
```

</details>
