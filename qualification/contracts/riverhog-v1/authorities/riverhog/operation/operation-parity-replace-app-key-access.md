# Operation parity: replace_app_key_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-replace-app-key-access:69a23d7c4f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [apps](families/apps/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-bbd74944c309"></a>
| Concern | Contract |
|---|---|
| <a id="s-13d366b54881"></a>`application` | riverhog |
| <a id="s-ec88c496329f"></a>`classification` | human-cli+json |
| <a id="s-8f71518debb8"></a>`cli_commands` | ["app key access set"] |
| <a id="s-1337d0cb6504"></a>`client` | ApiClient |
| <a id="s-158052d6ed0b"></a>`method` | PUT |
| <a id="s-dd59b3ed941c"></a>`operation_id` | replace_app_key_access |
| <a id="s-77a5de1c0f56"></a>`path` | /v1/apps/{app}/keys/{key_id}/access |
| <a id="s-30482cfdeb6c"></a>`provider_evidence` | None |
| <a id="s-f88938183dc5"></a>`read_collection` | None |
| <a id="s-0e2a1332adba"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [PUT /v1/apps/{app}/keys/{key_id}/access](../http/put-v1-apps-app-keys-key-id-access.md)
- [piggity app key access set](../../piggity/cli/piggity-app-key-access-set.md)

## Governing policies

- <a id="pa-62930acb11dd"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-7ad7f72247a5"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-455d0235c941"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/8`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d34604b9057a5d9abafc9590761ea18b299632c32cb91692d4613021fd606fe -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key access set"
  ],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "replace_app_key_access",
  "path": "/v1/apps/{app}/keys/{key_id}/access",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
