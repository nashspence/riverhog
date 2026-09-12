# Operation parity: list_retrieval_plan_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-retrieval-plan-files:50e13d9b10 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-plans](families/retrieval-plans/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-681b1b0e478f"></a>
| Concern | Contract |
|---|---|
| <a id="s-a0a784b8bba8"></a>`application` | riverhog |
| <a id="s-e9d4dc72a9aa"></a>`classification` | client-only-primitive |
| <a id="s-5b5061f12e3d"></a>`cli_commands` | ["local repair", "local sync"] |
| <a id="s-cb588026e420"></a>`client` | ApiClient |
| <a id="s-3ab378f97cb2"></a>`method` | GET |
| <a id="s-b5890b16ee0a"></a>`operation_id` | list_retrieval_plan_files |
| <a id="s-544f6b8e4960"></a>`path` | /v1/retrieval-plans/{plan_id}/files |
| <a id="s-695091fc1bc1"></a>`provider_evidence` | None |
| <a id="s-ef74299442e6"></a>`read_collection` | {"authority": "retrieval-plan-files", "cursor_parameter": "start_ordinal", "kind": "exact-authority-page", "limit_parameter": "page_size"} |
| <a id="s-9f874e971835"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-plans/{plan_id}/files](../http/get-v1-retrieval-plans-plan-id-files.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-4410788d2d7f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-a7690ac3ccd8"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-b836a1ebddf3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/106`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 043c8b8878386a291fe6560db9d407477a1c98d498251dcd6c183d41da197dbf -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_retrieval_plan_files",
  "path": "/v1/retrieval-plans/{plan_id}/files",
  "provider_evidence": null,
  "read_collection": {
    "authority": "retrieval-plan-files",
    "cursor_parameter": "start_ordinal",
    "kind": "exact-authority-page",
    "limit_parameter": "page_size"
  },
  "response_authority": "http-json"
}
```
