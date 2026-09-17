# riverhog_client.ApiClient.get_collection_provenance_verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-collection-bfdae0b10f:01efeeb145 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2599365818"></a>
- <a id="s-54e45381e1"></a>`distribution`: `riverhog-client`
- <a id="s-c6b38abf52"></a>`module`: `riverhog_client`
- <a id="s-d9d4c47c0c"></a>`name`: `get_collection_provenance_verification`
- <a id="s-c03406eafb"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-870dab3e5a"></a>`unit`: `member`

### Declared structure

- <a id="s-d1b3d6c759"></a>`kind`: `"method"`
- <a id="s-60eab60e62"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection provenance verification-show](../../piggity/cli/piggity-collection-provenance-verification-show.md)
- [piggity collection provenance verify](../../piggity/cli/piggity-collection-provenance-verify.md)
- [GET /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-verification.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-6b8be81246"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.get\_collection\_provenance\_verification](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1940)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_collection_provenance_verification`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50ec68d6f6daf7cb9cb68d78f236f69af76197490ecf255ce692ca421a9216ac -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_collection_provenance_verification",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
