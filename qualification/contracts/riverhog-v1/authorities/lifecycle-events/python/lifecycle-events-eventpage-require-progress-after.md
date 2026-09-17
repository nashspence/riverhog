# lifecycle_events.EventPage.require_progress_after

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-eventpage-require-progress-after:81613e2c1a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3eb68acdc0"></a>
- <a id="s-cb405da28a"></a>`distribution`: `lifecycle-events`
- <a id="s-d1e7d49e9f"></a>`module`: `lifecycle_events`
- <a id="s-d8de066d2e"></a>`name`: `require_progress_after`
- <a id="s-61eb7450f4"></a>`owner`: `lifecycle_events.EventPage`
- <a id="s-af3364df4a"></a>`unit`: `member`

### Declared structure

- <a id="s-05502e3bb1"></a>`kind`: `"method"`
- <a id="s-249606859a"></a>`signature`: `"\"(self, cursor: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [EventPage](lifecycle-events-eventpage.md)

## Governing policies

- <a id="pa-a112b52768"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources/authorities.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.EventPage.require_progress_after`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e920d7fdef2cfda1df3282c01f52f65264f573e6c482eef652bbccefc74ff0b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, cursor: 'str') -> 'None'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "require_progress_after",
  "owner": "lifecycle_events.EventPage",
  "unit": "member"
}
```

</details>
