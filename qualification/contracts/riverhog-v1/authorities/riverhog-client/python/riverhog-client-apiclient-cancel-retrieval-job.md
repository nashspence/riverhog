# riverhog_client.ApiClient.cancel_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-cancel-retrieval-job:11f70f1da9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-06dd88839d"></a>
- <a id="s-e6abceabc7"></a>`distribution`: `riverhog-client`
- <a id="s-3e5a316ef1"></a>`module`: `riverhog_client`
- <a id="s-7290495157"></a>`name`: `cancel_retrieval_job`
- <a id="s-56bdb7a9d8"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-e0bcd73edc"></a>`unit`: `member`

### Declared structure

- <a id="s-d13de6d9ca"></a>`kind`: `"method"`
- <a id="s-7fa8341498"></a>`signature`: `"\"(self, job_id: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity local evict](../../piggity/cli/piggity-local-evict.md)
- [piggity local remove](../../piggity/cli/piggity-local-remove.md)
- [DELETE /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/delete-v1-retrieval-jobs-job-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-f2a66b11b1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.cancel_retrieval_job](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L880)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.cancel_retrieval_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f5158152aa9c4b58c900710752bab791fe615c086e6917a1b4951bde793350c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "cancel_retrieval_job",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
