# riverhog_client.ApiClient.cancel_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-cancel-collecti-33ced8420a:59ce5a7add -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3fd44f9082"></a>
- <a id="s-0ace0346a4"></a>`distribution`: `riverhog-client`
- <a id="s-b7147939c1"></a>`module`: `riverhog_client`
- <a id="s-6479e9467e"></a>`name`: `cancel_collection_upload_session`
- <a id="s-4c8d045554"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-e9015a7caf"></a>`unit`: `member`

### Declared structure

- <a id="s-d5740f9c15"></a>`kind`: `"method"`
- <a id="s-eff3b3382f"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection upload cancel](../../piggity/cli/piggity-collection-upload-cancel.md)
- [POST /v1/collection-upload-sessions/{collection_id}/cancel](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-cancel.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-9a657a5af1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.cancel\_collection\_upload\_session](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1451)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.cancel_collection_upload_session`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a2126c6784a8e221c6d82bc9d2a248739410f4488664e48ce0e0261cb622884 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "cancel_collection_upload_session",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
