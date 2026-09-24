# riverhog_client.processing.ClaimedCollectionApi.renew_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-32520a39e6:02e3e32e97 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b9e617827"></a>
- <a id="s-10509763bd"></a>`distribution`: `riverhog-client`
- <a id="s-060817895d"></a>`module`: `riverhog_client.processing`
- <a id="s-4a7ef477ad"></a>`name`: `renew_retrieval_job`
- <a id="s-777dea1c6e"></a>`owner`: `riverhog_client.processing.ClaimedCollectionApi`
- <a id="s-e2d3004170"></a>`unit`: `member`

### Declared structure

- <a id="s-bd3fa0d393"></a>`kind`: `"method"`
- <a id="s-f0abaabc87"></a>`signature`: `"\"(self, job_id: 'str', *, lease_seconds: 'int') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-processing-claimedcollectionapi.md)

## Governing policies

- <a id="pa-ccd926dfb6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionApi.renew_retrieval_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b8d7e1e202841294e914be128d2b49eb70319a5042334ec4393f24aecb78e7d9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', *, lease_seconds: 'int') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "renew_retrieval_job",
  "owner": "riverhog_client.processing.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
