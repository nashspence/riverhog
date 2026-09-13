# stove0_api_client.Stove0ApiClient.list_admission_policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-list-ad-795ad67f73:e9b7dc109f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ef977a783f"></a>
| Field | Shape |
|---|---|
| <a id="s-491c294437"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-26367120a8"></a>`distribution` | "stove0-api-client" |
| <a id="s-25ae68f686"></a>`module` | "stove0_api_client" |
| <a id="s-099a922060"></a>`name` | "list_admission_policies" |
| <a id="s-4023b786e8"></a>`owner` | "stove0_api_client.Stove0ApiClient" |
| <a id="s-2daa3404d8"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_api_client.Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-1f75673305"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.list_admission_policies`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95cc2de6cf27b83056e0321a13a854765f35c61f722f7514105302995465242c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'AdmissionPolicyCatalogView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "list_admission_policies",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
