# riverhog_client.ApiClient.cancel_collection_provenance_verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-cancel-collecti-d2d61d8b71:f00ee39528 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-92e98f2115"></a>
- <a id="s-751bedf3d2"></a>`distribution`: `riverhog-client`
- <a id="s-9aea1debee"></a>`module`: `riverhog_client`
- <a id="s-4b826eaa7a"></a>`name`: `cancel_collection_provenance_verification`
- <a id="s-4c31b37d04"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-69cc871185"></a>`unit`: `member`

### Declared structure

- <a id="s-b6c37e99ba"></a>`kind`: `"method"`
- <a id="s-c0f1d73db9"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection provenance verification-cancel](../../piggity/cli/piggity-collection-provenance-verification-cancel.md)
- [DELETE /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/delete-v1-collections-collection-id-provenance-verification.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-e80ba36ba4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.cancel\_collection\_provenance\_verification](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1947)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.cancel_collection_provenance_verification`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6c5b1feb4cb15f155e0e486c1c50ce4476d1e977071064e9657cfa1c0b7cf2b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "cancel_collection_provenance_verification",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
