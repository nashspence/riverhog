# Operation parity: create_app_key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-app-key:2e14bc16f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [apps](families/apps/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-34220579f8"></a>
| Concern | Contract |
|---|---|
| <a id="s-60b008e98c"></a>`application` | riverhog |
| <a id="s-038c29894b"></a>`classification` | human-cli+json |
| <a id="s-69131c1535"></a>`cli_commands` | ["app key create"] |
| <a id="s-2d09dc5560"></a>`client` | ApiClient |
| <a id="s-52df1e38a6"></a>`method` | POST |
| <a id="s-0330926e8a"></a>`operation_id` | create_app_key |
| <a id="s-372266bdf1"></a>`path` | /v1/apps/{app}/keys |
| <a id="s-d3136e727f"></a>`provider_evidence` | None |
| <a id="s-5c2b62edad"></a>`read_collection` | None |
| <a id="s-0a265cfbed"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys](../http/post-v1-apps-app-keys.md)
- [piggity app key create](../../piggity/cli/piggity-app-key-create.md)

## Governing policies

- <a id="pa-a4f0b76bb3"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-fb546be82c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-0ed6ba6230"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bddbfa841df0908b1ed686ea31134595256a03378434c334859ca5e317513e1f -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key create"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_app_key",
  "path": "/v1/apps/{app}/keys",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
