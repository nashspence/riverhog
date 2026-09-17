# lifecycle_events.CloudEvent.validate_time

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-cloudevent-validate-time:3a26861630 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7b28b00b37"></a>
- <a id="s-70fb18ea27"></a>`distribution`: `lifecycle-events`
- <a id="s-aacad80f6d"></a>`module`: `lifecycle_events`
- <a id="s-e1d68b87cf"></a>`name`: `validate_time`
- <a id="s-8da3dfb573"></a>`owner`: `lifecycle_events.CloudEvent`
- <a id="s-e4f10e08bc"></a>`unit`: `member`

### Declared structure

- <a id="s-d2eca65f24"></a>`kind`: `"classmethod"`
- <a id="s-cda912e8ec"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [CloudEvent](lifecycle-events-cloudevent.md)

## Governing policies

- <a id="pa-ee0a298d31"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.CloudEvent.validate_time`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 355e9845c0bcf258cde8e185550e0507ae655d2d5a781d50c8075df401b713a9 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "validate_time",
  "owner": "lifecycle_events.CloudEvent",
  "unit": "member"
}
```

</details>
