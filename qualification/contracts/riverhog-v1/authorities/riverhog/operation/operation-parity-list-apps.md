# Operation parity: list_apps

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-apps:df911c5963 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [apps](families/apps/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0e541b55a010"></a>
| Concern | Contract |
|---|---|
| <a id="s-12b8de11935b"></a>`application` | riverhog |
| <a id="s-f0396bddc9bc"></a>`classification` | human-cli+json |
| <a id="s-20f2dce09135"></a>`cli_commands` | ["app list"] |
| <a id="s-dc523ffab486"></a>`client` | ApiClient |
| <a id="s-3886445c8efc"></a>`method` | GET |
| <a id="s-4c10d9bf94e7"></a>`operation_id` | list_apps |
| <a id="s-648ff801e904"></a>`path` | /v1/apps |
| <a id="s-2f04d8893b28"></a>`provider_evidence` | None |
| <a id="s-d29a77f999eb"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-bf3a8ae6e565"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/apps](../http/get-v1-apps.md)
- [piggity app list](../../piggity/cli/piggity-app-list.md)

## Governing policies

- <a id="pa-d5b94cb80650"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-0008f91f8865"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-4b80acd7bc15"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b77c27b35516e3397b4decedec0ea6c2e38c3883045268946dda0643acc7e48 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_apps",
  "path": "/v1/apps",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "http-json"
}
```
