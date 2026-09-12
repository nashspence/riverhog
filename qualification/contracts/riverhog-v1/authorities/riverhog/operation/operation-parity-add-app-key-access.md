# Operation parity: add_app_key_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-add-app-key-access:796064f06c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [apps](families/apps/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b8c6e5895d"></a>
| Concern | Contract |
|---|---|
| <a id="s-b549329990"></a>`application` | riverhog |
| <a id="s-b09c647e4d"></a>`classification` | human-cli+json |
| <a id="s-ba4041c8ad"></a>`cli_commands` | ["app key access add"] |
| <a id="s-c0d4cf23e9"></a>`client` | ApiClient |
| <a id="s-cdad7077fd"></a>`method` | POST |
| <a id="s-467adcdcdb"></a>`operation_id` | add_app_key_access |
| <a id="s-d34988952e"></a>`path` | /v1/apps/{app}/keys/{key_id}/access |
| <a id="s-75ff6626f1"></a>`provider_evidence` | None |
| <a id="s-893940100b"></a>`read_collection` | None |
| <a id="s-74053b5d7c"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/access](../http/post-v1-apps-app-keys-key-id-access.md)
- [piggity app key access add](../../piggity/cli/piggity-app-key-access-add.md)

## Governing policies

- <a id="pa-30fb9047e7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d0029d65eb"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-7224a09003"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/7`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5390c4f2dfb83da38f4bfc18b45c430b88bfad13f0b61f6b1555afebe9d5ac99 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key access add"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "add_app_key_access",
  "path": "/v1/apps/{app}/keys/{key_id}/access",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
