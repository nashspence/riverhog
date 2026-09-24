# riverhog_client.processing.ClaimedCollectionApi.list_retrieval_plan_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-23a053be33:ddc7e3cffb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-742e2f586b"></a>
- <a id="s-91d2c97026"></a>`distribution`: `riverhog-client`
- <a id="s-969426fe7b"></a>`module`: `riverhog_client.processing`
- <a id="s-76e1c50592"></a>`name`: `list_retrieval_plan_files`
- <a id="s-7b4b4d8f4a"></a>`owner`: `riverhog_client.processing.ClaimedCollectionApi`
- <a id="s-fb7e05a095"></a>`unit`: `member`

### Declared structure

- <a id="s-0bb8eb62ec"></a>`kind`: `"method"`
- <a id="s-acd8bb9c2e"></a>`signature`: `"\"(self, plan_id: 'str', *, plan_etag: 'str', start_ordinal: 'int' = 0, page_size: 'int' = 100) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-processing-claimedcollectionapi.md)

## Governing policies

- <a id="pa-82ba78a2e0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionApi.list_retrieval_plan_files`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41c5006e03de716adfbecee923501a546a874fb2baf737de62df49bb8424c745 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan_id: 'str', *, plan_etag: 'str', start_ordinal: 'int' = 0, page_size: 'int' = 100) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "list_retrieval_plan_files",
  "owner": "riverhog_client.processing.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
