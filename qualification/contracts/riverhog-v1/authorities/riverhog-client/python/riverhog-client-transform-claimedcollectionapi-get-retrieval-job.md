# riverhog_client.transform.ClaimedCollectionApi.get_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-1b5070d5da:b94413ab52 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8b7a4f4ef4"></a>
- <a id="s-0cb62c7346"></a>`distribution`: `riverhog-client`
- <a id="s-80494b46d1"></a>`module`: `riverhog_client.transform`
- <a id="s-21e7f1e9a8"></a>`name`: `get_retrieval_job`
- <a id="s-0771ac4b0d"></a>`owner`: `riverhog_client.transform.ClaimedCollectionApi`
- <a id="s-2ba711e0fc"></a>`unit`: `member`

### Declared structure

- <a id="s-14da4ee0bb"></a>`kind`: `"method"`
- <a id="s-49aa8789de"></a>`signature`: `"\"(self, job_id: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-transform-claimedcollectionapi.md)

## Governing policies

- <a id="pa-650895b666"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi.get_retrieval_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b69dedd9dd0f3bd0094d362500048193bca2018be5f93a08f692b542fa21962 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "get_retrieval_job",
  "owner": "riverhog_client.transform.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
