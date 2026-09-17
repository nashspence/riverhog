# stove0_api_client.Stove0ApiClient.scheduler_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-scheduler-status:ee5eeae3f0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-296c27d282"></a>
- <a id="s-f1ebfc5de1"></a>`distribution`: `stove0-api-client`
- <a id="s-a88e9987ac"></a>`module`: `stove0_api_client`
- <a id="s-e6721ec6f3"></a>`name`: `scheduler_status`
- <a id="s-4b89eab2a8"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-a660369490"></a>`unit`: `member`

### Declared structure

- <a id="s-63df1b4bff"></a>`kind`: `"method"`
- <a id="s-e8ea0d3931"></a>`signature`: `"\"(self) -> 'SchedulerStatus'\""`

## Maintained corroboration

### Related interface records

- [stove0 scheduler status](../../stove0-client/cli/stove0-scheduler-status.md)
- [GET /v1/admin/scheduler](../../stove0/http-operations/get-v1-admin-scheduler.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-c476fd5afa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [reference/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [reference/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.scheduler\_status](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L448)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.scheduler_status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fca49bb14085475a2e0288722643654d0a67a55b6afd1e79099805ac68f8e407 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'SchedulerStatus'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "scheduler_status",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
