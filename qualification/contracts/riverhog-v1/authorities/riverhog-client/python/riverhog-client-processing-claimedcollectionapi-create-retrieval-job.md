# riverhog_client.processing.ClaimedCollectionApi.create_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-f9160ff7a4:00c8fc2955 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f9c9836f78"></a>
- <a id="s-f1a44194f4"></a>`distribution`: `riverhog-client`
- <a id="s-0e7173092c"></a>`module`: `riverhog_client.processing`
- <a id="s-6f8c97126f"></a>`name`: `create_retrieval_job`
- <a id="s-96e0d3b8c1"></a>`owner`: `riverhog_client.processing.ClaimedCollectionApi`
- <a id="s-58f5c2bce5"></a>`unit`: `member`

### Declared structure

- <a id="s-ae46800706"></a>`kind`: `"method"`
- <a id="s-3e837cb84e"></a>`signature`: `"\"(self, plan_id: 'str', *, plan_etag: 'str', event_context: 'Mapping[str, Any] \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-processing-claimedcollectionapi.md)

## Governing policies

- <a id="pa-8cdd7701a3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionApi.create_retrieval_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b9baf1adc69aaaf837b8360aec853550cca15e03a10f5d1a040350e846c098fc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan_id: 'str', *, plan_etag: 'str', event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "create_retrieval_job",
  "owner": "riverhog_client.processing.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
