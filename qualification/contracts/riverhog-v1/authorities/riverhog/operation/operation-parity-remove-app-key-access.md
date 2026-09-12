# Operation parity: remove_app_key_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-remove-app-key-access:b4f11a480b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [apps](families/apps/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a5e6938032"></a>
| Concern | Contract |
|---|---|
| <a id="s-f07f66bb2e"></a>`application` | riverhog |
| <a id="s-df8f47fb2f"></a>`classification` | human-cli+json |
| <a id="s-bd844588ca"></a>`cli_commands` | ["app key access remove"] |
| <a id="s-afda802593"></a>`client` | ApiClient |
| <a id="s-c0534cf139"></a>`method` | DELETE |
| <a id="s-e7826c50ff"></a>`operation_id` | remove_app_key_access |
| <a id="s-d7aa15cef0"></a>`path` | /v1/apps/{app}/keys/{key_id}/access |
| <a id="s-801b20d0d2"></a>`provider_evidence` | None |
| <a id="s-7f0009ebfb"></a>`read_collection` | None |
| <a id="s-320683aa7f"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [DELETE /v1/apps/{app}/keys/{key_id}/access](../http/delete-v1-apps-app-keys-key-id-access.md)
- [piggity app key access remove](../../piggity/cli/piggity-app-key-access-remove.md)

## Governing policies

- <a id="pa-2aced81251"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-7bcbf0117f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-0506fbc9f8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/6`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 204b2eb6478fd030999853284413622b780548f95aa489dc610e1f1d6a60a166 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key access remove"
  ],
  "client": "ApiClient",
  "method": "DELETE",
  "operation_id": "remove_app_key_access",
  "path": "/v1/apps/{app}/keys/{key_id}/access",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
