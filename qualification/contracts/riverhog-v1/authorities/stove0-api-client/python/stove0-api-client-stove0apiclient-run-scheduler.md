# stove0_api_client.Stove0ApiClient.run_scheduler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-run-scheduler:75bc7aa201 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b343f2dc8c"></a>
| Field | Shape |
|---|---|
| <a id="s-d42d747e21"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-62b5e8cdc3"></a>`distribution` | "stove0-api-client" |
| <a id="s-91eeacb38c"></a>`module` | "stove0_api_client" |
| <a id="s-7cc40a1ea9"></a>`name` | "run_scheduler" |
| <a id="s-68b2ed32e0"></a>`owner` | "stove0_api_client.Stove0ApiClient" |
| <a id="s-751580699f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_api_client.Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-3ff887af30"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.run_scheduler`

### Exact owned JSON

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
