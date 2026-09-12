# Operation parity: list_app_keys

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-app-keys:48d9860661 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [apps](families/apps/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6d6d9fa2bf07"></a>
| Concern | Contract |
|---|---|
| <a id="s-72561b2a5612"></a>`application` | riverhog |
| <a id="s-fb77c5b12efe"></a>`classification` | human-cli+json |
| <a id="s-9a1a76ab9543"></a>`cli_commands` | ["app key list"] |
| <a id="s-399cfb8b7ffe"></a>`client` | ApiClient |
| <a id="s-80a371dc8d87"></a>`method` | GET |
| <a id="s-b571d9520d18"></a>`operation_id` | list_app_keys |
| <a id="s-34f2ea6a5144"></a>`path` | /v1/apps/{app}/keys |
| <a id="s-7bc0fe9a6f66"></a>`provider_evidence` | None |
| <a id="s-957cdae24605"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-90024fa97816"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/apps/{app}/keys](../http/get-v1-apps-app-keys.md)
- [piggity app key list](../../piggity/cli/piggity-app-key-list.md)

## Governing policies

- <a id="pa-df166a6bce4a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-2b1b79e674fc"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-4e4c1428dd99"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/4`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 885a61841dc5109beb79feaf2d8bf9fba3cec47b56c4380588e5afb89c103666 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_app_keys",
  "path": "/v1/apps/{app}/keys",
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
