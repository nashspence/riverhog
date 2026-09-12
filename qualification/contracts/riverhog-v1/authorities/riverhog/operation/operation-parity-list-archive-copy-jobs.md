# Operation parity: list_archive_copy_jobs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-archive-copy-jobs:beff5586ff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1d94de0077ea"></a>
| Concern | Contract |
|---|---|
| <a id="s-cc69c7720818"></a>`application` | riverhog |
| <a id="s-713762854204"></a>`classification` | human-cli+json |
| <a id="s-ae713cb60e86"></a>`cli_commands` | ["archive copy list"] |
| <a id="s-f0d775d6c43a"></a>`client` | ApiClient |
| <a id="s-941045f9d29b"></a>`method` | GET |
| <a id="s-3f8029b674ee"></a>`operation_id` | list_archive_copy_jobs |
| <a id="s-38dd3eea9ca8"></a>`path` | /v1/archive/copies |
| <a id="s-a4fd7546463a"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-2605fee670ed"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-c67f598e1bf3"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/copies](../http/get-v1-archive-copies.md)
- [piggity archive copy list](../../piggity/cli/piggity-archive-copy-list.md)

## Governing policies

- <a id="pa-8c5ced110091"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-dfb6a9549287"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-b233509c766f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/12`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 940cfe4de913b836ffffb96b3fe88ef48bc026d158be9ae585d06bad91d76166 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive copy list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_archive_copy_jobs",
  "path": "/v1/archive/copies",
  "provider_evidence": "provider-qualification:#442",
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
