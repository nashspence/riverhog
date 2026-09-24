# riverhog_client.processing.ClaimedCollectionApi.download_retrieval_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-c2dfab3704:402080da43 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ddfa84581"></a>
- <a id="s-9fab359a65"></a>`distribution`: `riverhog-client`
- <a id="s-1070995de8"></a>`module`: `riverhog_client.processing`
- <a id="s-ef18265a01"></a>`name`: `download_retrieval_file`
- <a id="s-52b89a10c5"></a>`owner`: `riverhog_client.processing.ClaimedCollectionApi`
- <a id="s-4bd8428dad"></a>`unit`: `member`

### Declared structure

- <a id="s-d9b12b5910"></a>`kind`: `"method"`
- <a id="s-2a3e4835b7"></a>`signature`: `"\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', output: 'Path', expected_bytes: 'int', expected_sha256: 'str') -> 'int'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-processing-claimedcollectionapi.md)

## Governing policies

- <a id="pa-b1ea019883"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionApi.download_retrieval_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3212dac504dc62c272c487d56283bd2464e026dc3028fcffe91af23fe97de5d2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', output: 'Path', expected_bytes: 'int', expected_sha256: 'str') -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "download_retrieval_file",
  "owner": "riverhog_client.processing.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
