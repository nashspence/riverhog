# riverhog_client.ApiClient.trace_collection_file_provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-trace-collectio-1373cfb9cd:f92f4b586b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9655e273f4"></a>
| Field | Shape |
|---|---|
| <a id="s-7555a94e20"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b92620696b"></a>`distribution` | "riverhog-client" |
| <a id="s-d499fc9033"></a>`module` | "riverhog_client" |
| <a id="s-5887d36439"></a>`name` | "trace_collection_file_provenance" |
| <a id="s-c01483aa0b"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-9734ac5761"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-c62e9ff4f9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.trace_collection_file_provenance`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84906f4883250b6cef6b18510cebdf9ab9a99edd4a423d28315a066fa4ef6267 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', path: 'str', *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "trace_collection_file_provenance",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
