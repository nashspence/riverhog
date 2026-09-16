# riverhog_client.transform.ClaimedCollectionApi.renew_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-0a9ddc0584:e52b920127 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9f21cb4015"></a>
- <a id="s-563cfd9cfb"></a>`distribution`: `riverhog-client`
- <a id="s-72a135d718"></a>`module`: `riverhog_client.transform`
- <a id="s-ff2ad39d8b"></a>`name`: `renew_retrieval_job`
- <a id="s-8ec02c2a3a"></a>`owner`: `riverhog_client.transform.ClaimedCollectionApi`
- <a id="s-db9f5cde7d"></a>`unit`: `member`

### Declared structure

- <a id="s-09fa99df2d"></a>`kind`: `"method"`
- <a id="s-5225940bbc"></a>`signature`: `"\"(self, job_id: 'str', *, lease_seconds: 'int') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-transform-claimedcollectionapi.md)

## Governing policies

- <a id="pa-122209951a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi.renew_retrieval_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a005ebba835749a27dcfcf34a3c148ff623a3feaa8861f28719fed517a16b956 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', *, lease_seconds: 'int') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "renew_retrieval_job",
  "owner": "riverhog_client.transform.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
