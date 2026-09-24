# riverhog_client.processing.ClaimedCollectionReader

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollectionreader:059debd310 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-54a5ee17d4"></a>
- <a id="s-91a47676db"></a>`distribution`: `riverhog-client`
- <a id="s-50f4161d23"></a>`module`: `riverhog_client.processing`
- <a id="s-2bde6d6de8"></a>`name`: `ClaimedCollectionReader`
- <a id="s-cf1b711668"></a>`unit`: `export`

### Declared structure

- <a id="s-48853b6d8f"></a>`kind`: `"class"`
- <a id="s-e832870163"></a>`signature`: `"\"(api: 'ClaimedCollectionApi', *, inputs: 'Sequence[CollectionRootIdentity]', work_id: 'str', claim_id: 'str', fence: 'int', heartbeat: 'Heartbeat \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [replace_api](riverhog-client-processing-claimedcollectionreader-replace-api.md)
- [prepare](riverhog-client-processing-claimedcollectionreader-prepare.md)
- [iter_inventory](riverhog-client-processing-claimedcollectionreader-iter-inventory.md)
- [close_retrievals](riverhog-client-processing-claimedcollectionreader-close-retrievals.md)

## Governing policies

- <a id="pa-2ff2faa19b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionReader`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 288856347b96ef5f284c7df8681757df9b0d81eaddf71f834b7175efccbb1881 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'ClaimedCollectionApi', *, inputs: 'Sequence[CollectionRootIdentity]', work_id: 'str', claim_id: 'str', fence: 'int', heartbeat: 'Heartbeat | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "ClaimedCollectionReader",
  "unit": "export"
}
```

</details>
