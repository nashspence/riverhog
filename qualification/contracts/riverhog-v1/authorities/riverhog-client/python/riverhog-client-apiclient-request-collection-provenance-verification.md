# riverhog_client.ApiClient.request_collection_provenance_verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-request-collect-a1f0b246dd:966203f682 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e8043dc4f0"></a>
- <a id="s-1a6960f6e1"></a>`distribution`: `riverhog-client`
- <a id="s-62608e3d79"></a>`module`: `riverhog_client`
- <a id="s-29b8aae373"></a>`name`: `request_collection_provenance_verification`
- <a id="s-3c5af8bf07"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-cea757c29e"></a>`unit`: `member`

### Declared structure

- <a id="s-972b915957"></a>`kind`: `"method"`
- <a id="s-7ee368aba5"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection provenance verify](../../a-riverhog-cli/cli/a-riverhog-cli-collection-provenance-verify.md)
- [POST /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/post-v1-collections-collection-id-provenance-verification.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-f2b0edd72a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.request\_collection\_provenance\_verification](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1953)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.request_collection_provenance_verification`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0efe7481d5f12ef419f8d24d7c65b5d48bd18ae7ce805200fbeb7e6d58257eec -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "request_collection_provenance_verification",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
