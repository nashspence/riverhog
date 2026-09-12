# Operation parity: list_archive_stores

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-archive-stores:0ddd22101c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-33c163362e2c"></a>
| Concern | Contract |
|---|---|
| <a id="s-3d883a0fde76"></a>`application` | riverhog |
| <a id="s-eeb49cf8c64c"></a>`classification` | human-cli+json |
| <a id="s-10ee6cbd2830"></a>`cli_commands` | ["archive store list"] |
| <a id="s-2b91088569ee"></a>`client` | ApiClient |
| <a id="s-ca5f71759f94"></a>`method` | GET |
| <a id="s-558633214fdc"></a>`operation_id` | list_archive_stores |
| <a id="s-bae80d180920"></a>`path` | /v1/archive/stores |
| <a id="s-01b346b4d403"></a>`provider_evidence` | None |
| <a id="s-7b177a954893"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-a6579c7dde28"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/stores](../http/get-v1-archive-stores.md)
- [piggity archive store list](../../piggity/cli/piggity-archive-store-list.md)

## Governing policies

- <a id="pa-1a79a21fb073"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-ad1447cb44ca"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-8c76e18f4cf9"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/18`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4925710e3a384d4d2146457400764e7f8e45b915a244707f45772f0c49761b7 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive store list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_archive_stores",
  "path": "/v1/archive/stores",
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
