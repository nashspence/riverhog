# stove0_api_client.Stove0ApiClient.run_scheduler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-run-scheduler:75bc7aa201 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b343f2dc8c"></a>
- <a id="s-62b5e8cdc3"></a>`distribution`: `stove0-api-client`
- <a id="s-91eeacb38c"></a>`module`: `stove0_api_client`
- <a id="s-7cc40a1ea9"></a>`name`: `run_scheduler`
- <a id="s-68b2ed32e0"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-751580699f"></a>`unit`: `member`

### Declared structure

- <a id="s-e46a36722c"></a>`kind`: `"method"`
- <a id="s-b5fec669e8"></a>`signature`: `"\"(self, *, role: 'SchedulerRole' = 'combined', work_limit: 'int' = 25) -> 'SchedulerRun'\""`

## Maintained corroboration

### Related interface records

- [stove0 scheduler run](../../a-stove0-cli/cli/stove0-scheduler-run.md)
- [POST /v1/admin/scheduler/run](../../stove0/http-operations/post-v1-admin-scheduler-run.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-3ff887af30"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.run\_scheduler](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py#L496)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.run_scheduler`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55c2d77d5d7989f9fd337d884a95a1ef05772cdef99e05bc255e2dfa157f69cf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, role: 'SchedulerRole' = 'combined', work_limit: 'int' = 25) -> 'SchedulerRun'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "run_scheduler",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
