# Operation parity: create_catalog_sync_checkpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-catalog-sync-checkpoint:ac2343d3c9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [catalog-sync](families/catalog-sync/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5db53a6afde0"></a>
| Concern | Contract |
|---|---|
| <a id="s-f8d93f42ac98"></a>`application` | riverhog |
| <a id="s-9e3c886fd6eb"></a>`classification` | client-only-primitive |
| <a id="s-66d9f4ddf4c6"></a>`cli_commands` | ["catalog-sync checkpoint"] |
| <a id="s-dac4e1504476"></a>`client` | ApiClient |
| <a id="s-92060ed9ae8e"></a>`method` | GET |
| <a id="s-ee8f01eec27a"></a>`operation_id` | create_catalog_sync_checkpoint |
| <a id="s-45a0d44402e0"></a>`path` | /v1/catalog-sync/checkpoint |
| <a id="s-680130c5c4eb"></a>`provider_evidence` | None |
| <a id="s-7c5c54533a51"></a>`read_collection` | None |
| <a id="s-90c768c96642"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog-sync/checkpoint](../http/get-v1-catalog-sync-checkpoint.md)
- [piggity catalog-sync checkpoint](../../piggity/cli/piggity-catalog-sync-checkpoint.md)

## Governing policies

- <a id="pa-bf14f8ea5cab"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-e787dd5eca8c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-d764625ea159"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/21`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af8b41ee7c4ea7c87741552923de4491d7047f4d0e63680b13da89181c557e61 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "catalog-sync checkpoint"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "create_catalog_sync_checkpoint",
  "path": "/v1/catalog-sync/checkpoint",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
