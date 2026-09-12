# Operation parity: list_retrieval_cache_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-retrieval-cache-objects:53add94057 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-cache](families/retrieval-cache/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0b89260f90a5"></a>
| Concern | Contract |
|---|---|
| <a id="s-6232fbe929e3"></a>`application` | riverhog |
| <a id="s-76dce3946160"></a>`classification` | human-cli+json |
| <a id="s-71ae7d331699"></a>`cli_commands` | ["retrieval cache list"] |
| <a id="s-8bb5d563ce00"></a>`client` | ApiClient |
| <a id="s-df0a58ee6062"></a>`method` | GET |
| <a id="s-7ff089b5e10a"></a>`operation_id` | list_retrieval_cache_objects |
| <a id="s-9ff40daba2a1"></a>`path` | /v1/retrieval-cache/objects |
| <a id="s-345f9d13ec4e"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-a6d14ef12a44"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-91a010f3f0aa"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-cache/objects](../http/get-v1-retrieval-cache-objects.md)
- [piggity retrieval cache list](../../piggity/cli/piggity-retrieval-cache-list.md)

## Governing policies

- <a id="pa-9da32fcef335"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-27479ec76460"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-673b1163855c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/94`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1926a5d532adc9194b9f2b2063f37706564b31283db1535fd9337eb2fba704dd -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "retrieval cache list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_retrieval_cache_objects",
  "path": "/v1/retrieval-cache/objects",
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
