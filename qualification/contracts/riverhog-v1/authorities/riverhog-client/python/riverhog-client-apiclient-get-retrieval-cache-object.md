# riverhog_client.ApiClient.get_retrieval_cache_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-retrieval-cache-object:cebbc4d0ff -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-273555662f"></a>
- <a id="s-7b6aebfd5d"></a>`distribution`: `riverhog-client`
- <a id="s-d3cc4d6d03"></a>`module`: `riverhog_client`
- <a id="s-f3ad74850f"></a>`name`: `get_retrieval_cache_object`
- <a id="s-dc3a06c29b"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-e021254b34"></a>`unit`: `member`

### Declared structure

- <a id="s-e5c35c799e"></a>`kind`: `"method"`
- <a id="s-0fa963587f"></a>`signature`: `"\"(self, collection_id: 'CollectionId', source_store: 'ArchiveStoreName', object_id: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity retrieval cache show](../../piggity/cli/piggity-retrieval-cache-show.md)
- [GET /v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}](../../riverhog/http-operations/get-v1-retrieval-cache-objects-collection-id-source-store-object-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-852bc2b671"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.get\_retrieval\_cache\_object](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L965)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_retrieval_cache_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b25188afe034cd0e57392e174d0c6f681611ef2c44925f319dd08e1c4abcc79f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', source_store: 'ArchiveStoreName', object_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_retrieval_cache_object",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
