# riverhog_client.ApiClient.delete_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-delete-collection:278db98af5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-766bdbf174"></a>
| Field | Shape |
|---|---|
| <a id="s-3e275e90e2"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-10ca79f743"></a>`distribution` | "riverhog-client" |
| <a id="s-b4c05a12a3"></a>`module` | "riverhog_client" |
| <a id="s-285a87dfb1"></a>`name` | "delete_collection" |
| <a id="s-f8b6d183b2"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-2c00777964"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-b6b93c98ab"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.delete_collection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f22e25e818b7955295fa53e195bb37ce8cd55f91f0569b58ccccc80ce1cb64fd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, challenge: 'str', retirement_claim_id: 'ProcessingClaimId | None' = None, event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "delete_collection",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
