# Operation parity: get_download_quota

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-download-quota:f146167eeb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [download-quota](families/download-quota/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d54550b673"></a>
| Concern | Contract |
|---|---|
| <a id="s-636e8fedbe"></a>`application` | riverhog |
| <a id="s-637898ca3d"></a>`classification` | human-cli+json |
| <a id="s-6ca0a6be9a"></a>`cli_commands` | ["app key quota show"] |
| <a id="s-e0284e55a4"></a>`client` | ApiClient |
| <a id="s-6163bd4319"></a>`method` | GET |
| <a id="s-f83a2b6650"></a>`operation_id` | get_download_quota |
| <a id="s-7da39ccdf8"></a>`path` | /v1/download-quota |
| <a id="s-2a2f4171cb"></a>`provider_evidence` | None |
| <a id="s-3f2d1395bb"></a>`read_collection` | None |
| <a id="s-a7f92ef7cb"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/download-quota](../http/get-v1-download-quota.md)
- [piggity app key quota show](../../piggity/cli/piggity-app-key-quota-show.md)

## Governing policies

- <a id="pa-563d44c213"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-17d4cabb92"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-9c7b3a6f6d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/90`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1934e1e5223ce0d568d120ef1049c3ad248db06f41904936bc51db603c4ede52 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key quota show"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_download_quota",
  "path": "/v1/download-quota",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
