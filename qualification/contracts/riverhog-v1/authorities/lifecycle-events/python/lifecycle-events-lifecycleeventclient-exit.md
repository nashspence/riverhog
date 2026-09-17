# lifecycle_events.LifecycleEventClient.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-lifecycleeventclient-exit:68a3a55059 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f9dbddaec0"></a>
- <a id="s-3d731fccb6"></a>`distribution`: `lifecycle-events`
- <a id="s-91407dd4f6"></a>`module`: `lifecycle_events`
- <a id="s-ad42213266"></a>`name`: `__exit__`
- <a id="s-b8ce713f07"></a>`owner`: `lifecycle_events.LifecycleEventClient`
- <a id="s-5035614707"></a>`unit`: `member`

### Declared structure

- <a id="s-26623af1da"></a>`kind`: `"method"`
- <a id="s-4410494934"></a>`signature`: `"\"(self, *_args: 'Any') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [LifecycleEventClient](lifecycle-events-lifecycleeventclient.md)

## Governing policies

- <a id="pa-08c7f5f520"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources/authorities.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.LifecycleEventClient.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1301fc095566395b0d91096b52938ec863207be2274dabb6f2b666a824134e34 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *_args: 'Any') -> 'None'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "__exit__",
  "owner": "lifecycle_events.LifecycleEventClient",
  "unit": "member"
}
```

</details>
