# riverhog_client.ApiClient.get_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-retrieval-job:4ee57b348f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-160014d459"></a>
- <a id="s-60cc7828b3"></a>`distribution`: `riverhog-client`
- <a id="s-294dbfbe40"></a>`module`: `riverhog_client`
- <a id="s-a6186dadf0"></a>`name`: `get_retrieval_job`
- <a id="s-ce86f62269"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-ad610be03b"></a>`unit`: `member`

### Declared structure

- <a id="s-eb6db1f872"></a>`kind`: `"method"`
- <a id="s-3cb8749e07"></a>`signature`: `"\"(self, job_id: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity local evict](../../piggity/cli/piggity-local-evict.md)
- [piggity local remove](../../piggity/cli/piggity-local-remove.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)
- [GET /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/get-v1-retrieval-jobs-job-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-ae2615f600"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.get_retrieval_job](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L875)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_retrieval_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ad98abd87bda1678c7ed81aa0b790970a2608f0ab03c4dff5e4de4f7174454d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_retrieval_job",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
