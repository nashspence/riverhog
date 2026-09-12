# Operation parity: get_ftp_adapter_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog-ftp-adapter:operation-parity-get-ftp-adapter-status:c02c54b051 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [operation](index.md) |
| Family | [status](index.md#f-5cf20b4299c4) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-10cb5a258515"></a>
| Concern | Contract |
|---|---|
| <a id="s-008670ee2025"></a>`application` | riverhog-ftp-adapter |
| <a id="s-72c56b98f7e2"></a>`classification` | human-cli+json |
| <a id="s-0002e0dd66b6"></a>`cli_commands` | ["status"] |
| <a id="s-f6c0b4a9ad4d"></a>`client` | RiverhogFtpAdapterClient |
| <a id="s-96ab76702f43"></a>`method` | GET |
| <a id="s-e1e4ef9dfc6e"></a>`operation_id` | get_ftp_adapter_status |
| <a id="s-a2c2192a803b"></a>`path` | /v1/status |
| <a id="s-7163035f2c9a"></a>`provider_evidence` | None |
| <a id="s-dd9c1d8b83b4"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-24b4413127f7"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/status](../http/get-v1-status.md)

## Governing policies

- <a id="pa-a26c021abd3b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-f8b5328c6de3"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-ae96e4923138"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/113`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d06aadedfd288a7e24fbbbdca307009dfcf50f37fb1b96d9b0b062ef03462436 -->

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "human-cli+json",
  "cli_commands": [
    "status"
  ],
  "client": "RiverhogFtpAdapterClient",
  "method": "GET",
  "operation_id": "get_ftp_adapter_status",
  "path": "/v1/status",
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
