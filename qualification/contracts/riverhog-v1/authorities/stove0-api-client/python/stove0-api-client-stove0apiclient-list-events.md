# stove0_api_client.Stove0ApiClient.list_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-list-events:e215328a73 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5c6c95db10"></a>
- <a id="s-487eaee4c0"></a>`distribution`: `stove0-api-client`
- <a id="s-e841c58d66"></a>`module`: `stove0_api_client`
- <a id="s-30749d572c"></a>`name`: `list_events`
- <a id="s-5e8bb0be12"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-a30a7875da"></a>`unit`: `member`

### Declared structure

- <a id="s-8678d18a2d"></a>`kind`: `"method"`
- <a id="s-76d24ae2ff"></a>`signature`: `"\"(self, *, after: 'str \| None' = None, limit: 'int' = 100) -> 'Stove0EventPage'\""`

## Maintained corroboration

### Related interface records

- [stove0 event list](../../a-stove0-cli/cli/stove0-event-list.md)
- [GET /v1/events](../../stove0/http-operations/get-v1-events.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-685d2d79f3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.list\_events](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py#L145)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.list_events`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0c019d91d71135c04919aab5f2d14363b5b22fdb9edc6c4fa1a820ce59ae2b1a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, after: 'str | None' = None, limit: 'int' = 100) -> 'Stove0EventPage'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "list_events",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
