# riverhog_client.processing.ClaimedCollectionApi.acknowledge_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-3fa5b4e2e8:3e7e59f312 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d128e86587"></a>
- <a id="s-3328584de0"></a>`distribution`: `riverhog-client`
- <a id="s-117923fbd4"></a>`module`: `riverhog_client.processing`
- <a id="s-313390d5c1"></a>`name`: `acknowledge_retrieval_job`
- <a id="s-955a1ee45c"></a>`owner`: `riverhog_client.processing.ClaimedCollectionApi`
- <a id="s-cd6fa65140"></a>`unit`: `member`

### Declared structure

- <a id="s-ac8f342dfa"></a>`kind`: `"method"`
- <a id="s-1ec3778845"></a>`signature`: `"\"(self, job_id: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-processing-claimedcollectionapi.md)

## Governing policies

- <a id="pa-b0b3dc9106"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionApi.acknowledge_retrieval_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25a9493fa9470a75a9f43e0f4e1fbc2bc55435952095a1e8305fc9556a141578 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "acknowledge_retrieval_job",
  "owner": "riverhog_client.processing.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
