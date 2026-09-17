# riverhog_client.ApiClient.get_collection_derivation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-collection-derivation:a3b8139b3e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-deb1efa332"></a>
- <a id="s-06e12c107c"></a>`distribution`: `riverhog-client`
- <a id="s-84aec9f7dd"></a>`module`: `riverhog_client`
- <a id="s-4d13ca755a"></a>`name`: `get_collection_derivation`
- <a id="s-63bd121438"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-5224637af7"></a>`unit`: `member`

### Declared structure

- <a id="s-cac7504647"></a>`kind`: `"method"`
- <a id="s-135fad3d8c"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'CollectionDerivationResponseDocument'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/derivation](../../riverhog/http-operations/get-v1-collections-collection-id-derivation.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-fffcd4279a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.get\_collection\_derivation](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L672)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_collection_derivation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8e1fffd4b181b7f9895415194e1c70ef383fc20934589030a4a87ca88604133c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'CollectionDerivationResponseDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_collection_derivation",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
