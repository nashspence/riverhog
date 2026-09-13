# riverhog_client.ApiClient.create_or_resume_archive_copy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-or-resum-01171bf6e1:da17c26d04 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3f1c426088"></a>
| Field | Shape |
|---|---|
| <a id="s-0fa8353563"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c13c6abff5"></a>`distribution` | "riverhog-client" |
| <a id="s-ada2e589a3"></a>`module` | "riverhog_client" |
| <a id="s-6d4533a4c4"></a>`name` | "create_or_resume_archive_copy" |
| <a id="s-b4d4987a20"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-f1ec8f158e"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-7acdf795d4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_or_resume_archive_copy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90cedef088550cf5ce03770838692c5104dabe18e573f779a2ca780a6e4c030c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName', source_store: 'ArchiveStoreName | None' = None, event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_or_resume_archive_copy",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
