# riverhog_client.ApiClient.acknowledge_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-acknowledge-retrieval-job:2a9006654b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0fc55108e8"></a>
- <a id="s-9390e36022"></a>`distribution`: `riverhog-client`
- <a id="s-1efde8b802"></a>`module`: `riverhog_client`
- <a id="s-dafc1626f9"></a>`name`: `acknowledge_retrieval_job`
- <a id="s-339c5505ac"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-bfb77fdb37"></a>`unit`: `member`

### Declared structure

- <a id="s-7006c6b620"></a>`kind`: `"method"`
- <a id="s-f52b918860"></a>`signature`: `"\"(self, job_id: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-b1b1626481"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.acknowledge_retrieval_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02e420ab5c1dfb2077e07ed4b411026612c68b98ddb430e4efe017006e444454 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "acknowledge_retrieval_job",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
