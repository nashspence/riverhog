# lifecycle_events.LifecycleEventClient.page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-lifecycleeventclient-page:61ce4c776d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-08d5610148"></a>
- <a id="s-8695f4ab89"></a>`distribution`: `lifecycle-events`
- <a id="s-2552acd5ed"></a>`module`: `lifecycle_events`
- <a id="s-58d3d4cf09"></a>`name`: `page`
- <a id="s-56c2544207"></a>`owner`: `lifecycle_events.LifecycleEventClient`
- <a id="s-205bb241d6"></a>`unit`: `member`

### Declared structure

- <a id="s-2bd33a9c91"></a>`kind`: `"method"`
- <a id="s-8873dfbec5"></a>`signature`: `"\"(self, *, after: 'str \| None', limit: 'int' = 100) -> 'EventPage'\""`

## Maintained corroboration

### Related interface records

- [LifecycleEventClient](lifecycle-events-lifecycleeventclient.md)

## Governing policies

- <a id="pa-b802bd2d20"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources/authorities.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.LifecycleEventClient.page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ee1394f91bf2e44ead1267fca87f0848564a3cbbc2cf6c8e3f73f68af50fd0a0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, after: 'str | None', limit: 'int' = 100) -> 'EventPage'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "page",
  "owner": "lifecycle_events.LifecycleEventClient",
  "unit": "member"
}
```

</details>
