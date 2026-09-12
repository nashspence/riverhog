# Operation parity: list_catalog_sync_changes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-catalog-sync-changes:807d39cb32 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [catalog-sync](families/catalog-sync/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b194420518ea"></a>
| Concern | Contract |
|---|---|
| <a id="s-7e9ea1d4113f"></a>`application` | riverhog |
| <a id="s-63d840e9b0e7"></a>`classification` | human-cli+json |
| <a id="s-7a5f0e30a55a"></a>`cli_commands` | ["catalog-sync changes"] |
| <a id="s-3f2e1c0252be"></a>`client` | ApiClient |
| <a id="s-382d9c32ca7e"></a>`method` | GET |
| <a id="s-991e1f05f2be"></a>`operation_id` | list_catalog_sync_changes |
| <a id="s-f6b82fd01539"></a>`path` | /v1/catalog-sync/changes |
| <a id="s-d3da863f7ca6"></a>`provider_evidence` | None |
| <a id="s-736dd0bb0330"></a>`read_collection` | {"cursor_parameter": "cursor", "kind": "cursor-feed", "limit_parameter": "limit"} |
| <a id="s-1b2729ab43e4"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog-sync/changes](../http/get-v1-catalog-sync-changes.md)
- [piggity catalog-sync changes](../../piggity/cli/piggity-catalog-sync-changes.md)

## Governing policies

- <a id="pa-0c8df810d202"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-9d766e2f2013"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-6284ab3ac668"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/20`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b378a5e8238e25c0c199abb1583169cd7e8ea53f23b1c18e87e2a31f15582491 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "catalog-sync changes"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_catalog_sync_changes",
  "path": "/v1/catalog-sync/changes",
  "provider_evidence": null,
  "read_collection": {
    "cursor_parameter": "cursor",
    "kind": "cursor-feed",
    "limit_parameter": "limit"
  },
  "response_authority": "canonical-document"
}
```
