# riverhog_client.ApiClient.get_upload_copy_intents

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-upload-copy-intents:6dd7781f79 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c2fc88ea2d"></a>
- <a id="s-5c4dfe5b9c"></a>`distribution`: `riverhog-client`
- <a id="s-fe38ca9b8f"></a>`module`: `riverhog_client`
- <a id="s-73c4986fb9"></a>`name`: `get_upload_copy_intents`
- <a id="s-4b8f8222e6"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-edb44a17ec"></a>`unit`: `member`

### Declared structure

- <a id="s-ba4cebdebe"></a>`kind`: `"method"`
- <a id="s-f71a278f85"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli archive copy-job intents](../../a-riverhog-cli/cli/a-riverhog-cli-archive-copy-job-intents.md)
- [GET /v1/archive/upload-copy-intents/{collection_id}](../../riverhog/http-operations/get-v1-archive-upload-copy-intents-collection-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-33748dac88"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.get\_upload\_copy\_intents](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2469)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_upload_copy_intents`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08a04ba5be4682522dc2b2e21e0fa9eba7f43573d4a4757b57bef46cf461aa46 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_upload_copy_intents",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
