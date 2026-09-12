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

<a id="s-bbd74944c3"></a>
| Concern | Contract |
|---|---|
| <a id="s-13d366b548"></a>`application` | riverhog |
| <a id="s-ec88c49632"></a>`classification` | human-cli+json |
| <a id="s-8f71518deb"></a>`cli_commands` | ["app key access set"] |
| <a id="s-1337d0cb65"></a>`client` | ApiClient |
| <a id="s-158052d6ed"></a>`method` | PUT |
| <a id="s-dd59b3ed94"></a>`operation_id` | replace_app_key_access |
| <a id="s-77a5de1c0f"></a>`path` | /v1/apps/{app}/keys/{key_id}/access |
| <a id="s-30482cfdeb"></a>`provider_evidence` | None |
| <a id="s-f88938183d"></a>`read_collection` | None |
| <a id="s-0e2a1332ad"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [PUT /v1/apps/{app}/keys/{key_id}/access](../http/put-v1-apps-app-keys-key-id-access.md)
- [piggity app key access set](../../piggity/cli/piggity-app-key-access-set.md)

## Governing policies

- <a id="pa-62930acb11"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-7ad7f72247"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-455d0235c9"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

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
