# Operation parity: list_download_quotas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-download-quotas:b2e37de51f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [download-quotas](families/download-quotas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7e5769b54a"></a>
| Concern | Contract |
|---|---|
| <a id="s-dfc5366bf3"></a>`application` | riverhog |
| <a id="s-dadc7dd584"></a>`classification` | human-cli+json |
| <a id="s-3158c25c1a"></a>`cli_commands` | ["app key quota list"] |
| <a id="s-0d6ae69b13"></a>`client` | ApiClient |
| <a id="s-8d4da81b05"></a>`method` | GET |
| <a id="s-5cd1103cc2"></a>`operation_id` | list_download_quotas |
| <a id="s-c2af2a257a"></a>`path` | /v1/download-quotas |
| <a id="s-2723a1b03b"></a>`provider_evidence` | None |
| <a id="s-8ed9430608"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-7322cbcb16"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/download-quotas](../http/get-v1-download-quotas.md)
- [piggity app key quota list](../../piggity/cli/piggity-app-key-quota-list.md)

## Governing policies

- <a id="pa-3d345855ed"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-83aa98db7e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-f81642f81c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/91`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a32f8d22b1f745a6a8ad4e74c75a318a57c1d25b48babfec0648eb8859ee9f0a -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key quota list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_download_quotas",
  "path": "/v1/download-quotas",
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
