# Operation parity: list_catalog_sync_collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-catalog-sync-collections:e309737dfc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [catalog-sync](families/catalog-sync/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-68271d6461b8"></a>
| Concern | Contract |
|---|---|
| <a id="s-f842a02ab97c"></a>`application` | riverhog |
| <a id="s-8fffa6123db5"></a>`classification` | human-cli+json |
| <a id="s-b2f152f9bac3"></a>`cli_commands` | ["catalog-sync collections"] |
| <a id="s-214a19c4e97d"></a>`client` | ApiClient |
| <a id="s-a7484ccad024"></a>`method` | GET |
| <a id="s-9f49e9ddc03e"></a>`operation_id` | list_catalog_sync_collections |
| <a id="s-921e4f422591"></a>`path` | /v1/catalog-sync/collections |
| <a id="s-a3533b3204a1"></a>`provider_evidence` | None |
| <a id="s-07bce4d8e03a"></a>`read_collection` | {"authority": "catalog-sync-bootstrap", "cursor_parameter": "cursor", "kind": "exact-authority-page", "limit_parameter": "limit"} |
| <a id="s-0d7add51080a"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog-sync/collections](../http/get-v1-catalog-sync-collections.md)
- [piggity catalog-sync collections](../../piggity/cli/piggity-catalog-sync-collections.md)

## Governing policies

- <a id="pa-668cfd10ac1f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-a9dd353ed469"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-1972fe4b0812"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/22`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22d10a82c92bd18d6191764e205b850f51ffa4d2eb59bd601648c38b35bd2e78 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "catalog-sync collections"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_catalog_sync_collections",
  "path": "/v1/catalog-sync/collections",
  "provider_evidence": null,
  "read_collection": {
    "authority": "catalog-sync-bootstrap",
    "cursor_parameter": "cursor",
    "kind": "exact-authority-page",
    "limit_parameter": "limit"
  },
  "response_authority": "canonical-document"
}
```
