# Operation parity: list_app_key_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-app-key-access:db2dd5aff1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [app-key-access](families/app-key-access/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-942adac7784b"></a>
| Concern | Contract |
|---|---|
| <a id="s-e1b415d1aa5c"></a>`application` | riverhog |
| <a id="s-de3a75004bce"></a>`classification` | human-cli+json |
| <a id="s-9f1b81a92b2b"></a>`cli_commands` | ["app key access list"] |
| <a id="s-45a7217e9b49"></a>`client` | ApiClient |
| <a id="s-dc11770ec09c"></a>`method` | GET |
| <a id="s-35ad0b5f15c5"></a>`operation_id` | list_app_key_access |
| <a id="s-864c906c7155"></a>`path` | /v1/app-key-access |
| <a id="s-d4f5d5911225"></a>`provider_evidence` | None |
| <a id="s-cd526c17b28e"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-4f6aa56265ae"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/app-key-access](../http/get-v1-app-key-access.md)
- [piggity app key access list](../../piggity/cli/piggity-app-key-access-list.md)

## Governing policies

- <a id="pa-10a21513a433"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-a59728dab1b1"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-5174e57ce4d7"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a4c5faab4cddecba1b13f8c91dd05bd7026265d5e73734bcc71085b83c5c7803 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key access list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_app_key_access",
  "path": "/v1/app-key-access",
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
