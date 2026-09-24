# riverhog_client.processing.ClaimedCollectionApi.stream_retrieval_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-9db27725df:67a1c53168 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e5886c0e61"></a>
- <a id="s-091a3df74f"></a>`distribution`: `riverhog-client`
- <a id="s-2c8f8b1591"></a>`module`: `riverhog_client.processing`
- <a id="s-a69b1af61c"></a>`name`: `stream_retrieval_file`
- <a id="s-e04f323dfa"></a>`owner`: `riverhog_client.processing.ClaimedCollectionApi`
- <a id="s-cff36f705e"></a>`unit`: `member`

### Declared structure

- <a id="s-d76b862c07"></a>`kind`: `"method"`
- <a id="s-8a656e5946"></a>`signature`: `"\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', expected_bytes: 'int', expected_sha256: 'str', start: 'int' = 0, end: 'int \| None' = None, chunk_size: 'int' = 8388608) -> 'AbstractContextManager[Iterator[bytes]]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-processing-claimedcollectionapi.md)

## Governing policies

- <a id="pa-79a0575061"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionApi.stream_retrieval_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08bd14cbb8bf8375aae3cb74f209bb85a3d5170a0a542f701a5c879b39957714 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', expected_bytes: 'int', expected_sha256: 'str', start: 'int' = 0, end: 'int | None' = None, chunk_size: 'int' = 8388608) -> 'AbstractContextManager[Iterator[bytes]]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "stream_retrieval_file",
  "owner": "riverhog_client.processing.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
