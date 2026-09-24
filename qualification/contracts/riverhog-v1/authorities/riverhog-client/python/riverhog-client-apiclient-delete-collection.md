# riverhog_client.ApiClient.delete_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-delete-collection:278db98af5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-766bdbf174"></a>
- <a id="s-10ca79f743"></a>`distribution`: `riverhog-client`
- <a id="s-b4c05a12a3"></a>`module`: `riverhog_client`
- <a id="s-285a87dfb1"></a>`name`: `delete_collection`
- <a id="s-f8b6d183b2"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-2c00777964"></a>`unit`: `member`

### Declared structure

- <a id="s-158bac1cef"></a>`kind`: `"method"`
- <a id="s-7806312f9f"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, challenge: 'str', source_collection_retirement_claim_id: 'ProcessingClaimId \| None' = None, event_context: 'Mapping[str, Any] \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection delete](../../a-riverhog-cli/cli/a-riverhog-cli-collection-delete.md)
- [POST /v1/collections/{collection_id}/delete](../../riverhog/http-operations/post-v1-collections-collection-id-delete.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-b6b93c98ab"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.delete\_collection](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2000)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.delete_collection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebc041d9e384755a67325ee4c7dbe9664abd94930fe8f4994d48837adc66c465 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, challenge: 'str', source_collection_retirement_claim_id: 'ProcessingClaimId | None' = None, event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "delete_collection",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
