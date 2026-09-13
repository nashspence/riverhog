# lifecycle_events.CloudEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-cloudevent:3795582d07 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1177f7ab8d"></a>
| Field | Shape |
|---|---|
| <a id="s-cf30eb2307"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-a7b4f3e4bc"></a>`distribution` | "lifecycle-events" |
| <a id="s-ad7bf5ebbf"></a>`module` | "lifecycle_events" |
| <a id="s-2f03e10a66"></a>`name` | "CloudEvent" |
| <a id="s-19c004df0d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [lifecycle_events.CloudEvent.validate_time](lifecycle-events-cloudevent-validate-time.md)

## Governing policies

- <a id="pa-b731b7c842"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.CloudEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 643b114311801e94330500dfae5d8d46c141a3df71e3ac802231ecb30b9c940f -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "4f1bdc2cc9c131a7c8aad0b1bbe56f7bf3cf1f2c4a12a0727d577333cfc771c8",
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Annotated[str, MinLen(min_length=1)], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str | None, MinLen(min_length=1)] = None, time: str, datacontenttype: Literal['application/json'] = 'application/json', data: dict[str, typing.Any] = <factory>) -> None\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "CloudEvent",
  "unit": "export"
}
```
