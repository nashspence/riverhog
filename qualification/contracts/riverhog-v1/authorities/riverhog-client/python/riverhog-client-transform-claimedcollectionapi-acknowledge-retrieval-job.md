# riverhog_client.transform.ClaimedCollectionApi.acknowledge_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-18975d271e:6eabbb7e20 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a2a60ccb2a"></a>
- <a id="s-71670e1706"></a>`distribution`: `riverhog-client`
- <a id="s-810637d0a2"></a>`module`: `riverhog_client.transform`
- <a id="s-7b44a0ea48"></a>`name`: `acknowledge_retrieval_job`
- <a id="s-575132c284"></a>`owner`: `riverhog_client.transform.ClaimedCollectionApi`
- <a id="s-b6b739871f"></a>`unit`: `member`

### Declared structure

- <a id="s-2611ba0402"></a>`kind`: `"method"`
- <a id="s-f9b1d38fe9"></a>`signature`: `"\"(self, job_id: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionApi](riverhog-client-transform-claimedcollectionapi.md)

## Governing policies

- <a id="pa-b5c26458e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi.acknowledge_retrieval_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f97b5e024cd411a12f8e018f2618875bbca1508b427e1ae0518ed1998655473 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "acknowledge_retrieval_job",
  "owner": "riverhog_client.transform.ClaimedCollectionApi",
  "unit": "member"
}
```
