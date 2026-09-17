# lifecycle_events.LifecycleEventClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-lifecycleeventclient:2fee16e967 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9058ce1c58"></a>
- <a id="s-73767b7ecd"></a>`distribution`: `lifecycle-events`
- <a id="s-e4edb60763"></a>`module`: `lifecycle_events`
- <a id="s-e4f350ca64"></a>`name`: `LifecycleEventClient`
- <a id="s-be68f9ea04"></a>`unit`: `export`

### Declared structure

- <a id="s-4cd5048133"></a>`kind`: `"class"`
- <a id="s-b45d770d6a"></a>`signature`: `"\"(events_url: 'str', *, token: 'str', timeout: 'float' = 10.0, client: 'httpx.Client \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [close](lifecycle-events-lifecycleeventclient-close.md)
- [__enter__](lifecycle-events-lifecycleeventclient-enter.md)
- [__exit__](lifecycle-events-lifecycleeventclient-exit.md)
- [page](lifecycle-events-lifecycleeventclient-page.md)

## Governing policies

- <a id="pa-9787592011"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.LifecycleEventClient`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5596a659d18ba1fe0ff04168120e5058f5378f45d07298764d60118d9578e00b -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(events_url: 'str', *, token: 'str', timeout: 'float' = 10.0, client: 'httpx.Client | None' = None) -> 'None'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "LifecycleEventClient",
  "unit": "export"
}
```

</details>
