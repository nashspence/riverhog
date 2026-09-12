# Operation parity: get_archive_store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-archive-store:a06f6b9b81 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4bd96b09d672"></a>
| Concern | Contract |
|---|---|
| <a id="s-c00c557181ce"></a>`application` | riverhog |
| <a id="s-40caa677f8c0"></a>`classification` | human-cli+json |
| <a id="s-e4f6395bb49a"></a>`cli_commands` | ["archive store show"] |
| <a id="s-217883c16242"></a>`client` | ApiClient |
| <a id="s-e35c94929adf"></a>`method` | GET |
| <a id="s-35ec7cd00efd"></a>`operation_id` | get_archive_store |
| <a id="s-21845d07b013"></a>`path` | /v1/archive/stores/{store} |
| <a id="s-96e08fe6c182"></a>`provider_evidence` | None |
| <a id="s-f56685405821"></a>`read_collection` | None |
| <a id="s-302798552b56"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/stores/{store}](../http/get-v1-archive-stores-store.md)
- [piggity archive store show](../../piggity/cli/piggity-archive-store-show.md)

## Governing policies

- <a id="pa-c42c84dc39ca"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-a115b622b005"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-5908a1eac4e0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/19`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 71a948d7a22290a138112f74eeb39455b2f2fefdc5ea52a1041a9e8572fa62da -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive store show"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_archive_store",
  "path": "/v1/archive/stores/{store}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
