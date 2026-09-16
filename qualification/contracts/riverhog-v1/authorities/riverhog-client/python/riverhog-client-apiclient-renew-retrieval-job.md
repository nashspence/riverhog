# riverhog_client.ApiClient.renew_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-renew-retrieval-job:7b909d1477 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de692afdd8"></a>
- <a id="s-97ba6f1df1"></a>`distribution`: `riverhog-client`
- <a id="s-2d0d729188"></a>`module`: `riverhog_client`
- <a id="s-0721090a21"></a>`name`: `renew_retrieval_job`
- <a id="s-2a4ffea178"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-27bbbee2d1"></a>`unit`: `member`

### Declared structure

- <a id="s-15430818fd"></a>`kind`: `"method"`
- <a id="s-20f605eaec"></a>`signature`: `"\"(self, job_id: 'str', *, lease_seconds: 'int') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)
- [POST /v1/retrieval-jobs/{job_id}/renew](../../riverhog/http-operations/post-v1-retrieval-jobs-job-id-renew.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-f35ebf414b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.renew_retrieval_job](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L890)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.renew_retrieval_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95c885cb62684d02b2ead0b37b895be22d490d1228e056d9de87e517dc4a0d60 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', *, lease_seconds: 'int') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "renew_retrieval_job",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
