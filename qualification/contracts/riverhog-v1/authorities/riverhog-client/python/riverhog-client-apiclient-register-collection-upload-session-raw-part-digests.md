# riverhog_client.ApiClient.register_collection_upload_session_raw_part_digests

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-register-collec-ac44800b20:e8114f31fa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2631c7b201"></a>
- <a id="s-be28fd10f5"></a>`distribution`: `riverhog-client`
- <a id="s-638d1e84de"></a>`module`: `riverhog_client`
- <a id="s-f8ac9599a5"></a>`name`: `register_collection_upload_session_raw_part_digests`
- <a id="s-5bd68eaed3"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-56b97ef062"></a>`unit`: `member`

### Declared structure

- <a id="s-09c6c42bd8"></a>`kind`: `"method"`
- <a id="s-ce220afa68"></a>`signature`: `"\"(self, collection_id: 'CollectionId', batch: 'CollectionUploadRawDigestBatchDocument \| Mapping[str, Any]') -> 'CollectionUploadRawDigestProgressDocument'\""`

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [POST /v1/collection-upload-sessions/{collection_id}/raw-part-digests](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-raw-part-digests.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-48266c0f2e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.register\_collection\_upload\_session\_raw\_part\_digests](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1233)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.register_collection_upload_session_raw_part_digests`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c92bcb14bae0920fee08e6295e794f0f8d74f51814945c5887acc96eb525e56 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', batch: 'CollectionUploadRawDigestBatchDocument | Mapping[str, Any]') -> 'CollectionUploadRawDigestProgressDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "register_collection_upload_session_raw_part_digests",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
