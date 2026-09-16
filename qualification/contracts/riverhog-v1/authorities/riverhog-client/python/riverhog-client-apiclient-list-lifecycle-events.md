# riverhog_client.ApiClient.list_lifecycle_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-lifecycle-events:7924e9839d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cd1011876f"></a>
- <a id="s-faef0cf78e"></a>`distribution`: `riverhog-client`
- <a id="s-6f1cb4f80b"></a>`module`: `riverhog_client`
- <a id="s-f851d971db"></a>`name`: `list_lifecycle_events`
- <a id="s-5ebaf3b052"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-f3a7debf87"></a>`unit`: `member`

### Declared structure

- <a id="s-c0a91604bf"></a>`kind`: `"method"`
- <a id="s-46272689da"></a>`signature`: `"\"(self, *, after: 'LifecycleEventCursor \| None' = None, limit: 'int' = 100) -> 'RiverhogEventPage'\""`

## Maintained corroboration

### Related interface records

- [piggity event list](../../piggity/cli/piggity-event-list.md)
- [GET /v1/events](../../riverhog/http-operations/get-v1-events.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-0f9b3c20dd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.list_lifecycle_events](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L720)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_lifecycle_events`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8bb1120d95766a22244d9747f3a36c397839df38d44eb909b9ca355a681f3e14 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, after: 'LifecycleEventCursor | None' = None, limit: 'int' = 100) -> 'RiverhogEventPage'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_lifecycle_events",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
