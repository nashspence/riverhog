# Operation parity: set_app_key_download_quota

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-set-app-key-download-quota:14201c493d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [apps](families/apps/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1a70dcb61e"></a>
| Concern | Contract |
|---|---|
| <a id="s-24544d9f95"></a>`application` | riverhog |
| <a id="s-6d90f8ddbd"></a>`classification` | human-cli+json |
| <a id="s-7d47619c01"></a>`cli_commands` | ["app key quota set"] |
| <a id="s-218167ced8"></a>`client` | ApiClient |
| <a id="s-ce4fe88ba0"></a>`method` | PUT |
| <a id="s-71fa5f2bbb"></a>`operation_id` | set_app_key_download_quota |
| <a id="s-38e63abc8a"></a>`path` | /v1/apps/{app}/keys/{key_id}/download-quota |
| <a id="s-3a8218d400"></a>`provider_evidence` | None |
| <a id="s-ae00c73f09"></a>`read_collection` | None |
| <a id="s-231222e6d4"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [PUT /v1/apps/{app}/keys/{key_id}/download-quota](../http/put-v1-apps-app-keys-key-id-download-quota.md)
- [piggity app key quota set](../../piggity/cli/piggity-app-key-quota-set.md)

## Governing policies

- <a id="pa-77090ab8ad"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-7672fecd84"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-a8e7bc2ea0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/9`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2a4ce7aa2243bbe80d31d61af9830710799fe00be7d579d1d2cca175d3ece47 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key quota set"
  ],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "set_app_key_download_quota",
  "path": "/v1/apps/{app}/keys/{key_id}/download-quota",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
