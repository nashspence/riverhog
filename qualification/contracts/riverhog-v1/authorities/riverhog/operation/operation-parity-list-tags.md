# Operation parity: list_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-tags:8e381cec10 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [tags](families/tags/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c732f678ad20"></a>
| Concern | Contract |
|---|---|
| <a id="s-a2487b29cd53"></a>`application` | riverhog |
| <a id="s-2d8cda9def06"></a>`classification` | human-cli+json |
| <a id="s-f2abb6f7a74a"></a>`cli_commands` | ["tag list"] |
| <a id="s-6c526c2294b0"></a>`client` | ApiClient |
| <a id="s-c840c2f882db"></a>`method` | GET |
| <a id="s-d36c99b7bea7"></a>`operation_id` | list_tags |
| <a id="s-fcc2a985e366"></a>`path` | /v1/tags |
| <a id="s-4f6316c3beef"></a>`provider_evidence` | None |
| <a id="s-f27ead3538cc"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-f10794c72b17"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/tags](../http/get-v1-tags.md)
- [piggity tag list](../../piggity/cli/piggity-tag-list.md)

## Governing policies

- <a id="pa-ab879e6d77f9"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-8a0a543fb7eb"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-b01e160162cc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/108`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1f89970f08589032957f738febce997a79d674f8ef46b8849130245967955881 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "tag list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_tags",
  "path": "/v1/tags",
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
