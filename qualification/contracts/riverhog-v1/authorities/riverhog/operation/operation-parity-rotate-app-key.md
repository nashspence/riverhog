# Operation parity: rotate_app_key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-rotate-app-key:d175804ecd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [apps](families/apps/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-15bac6a5a1"></a>
| Concern | Contract |
|---|---|
| <a id="s-fef82303d6"></a>`application` | riverhog |
| <a id="s-5f868f7708"></a>`classification` | human-cli+json |
| <a id="s-da1ef93a12"></a>`cli_commands` | ["app key rotate"] |
| <a id="s-2869f1b458"></a>`client` | ApiClient |
| <a id="s-977bdebe40"></a>`method` | POST |
| <a id="s-308b380813"></a>`operation_id` | rotate_app_key |
| <a id="s-614d6a78b1"></a>`path` | /v1/apps/{app}/keys/{key_id}/rotate |
| <a id="s-747bbd16fd"></a>`provider_evidence` | None |
| <a id="s-bcb0108e15"></a>`read_collection` | None |
| <a id="s-bb73251e22"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/rotate](../http/post-v1-apps-app-keys-key-id-rotate.md)
- [piggity app key rotate](../../piggity/cli/piggity-app-key-rotate.md)

## Governing policies

- <a id="pa-6f629777cb"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-7b9f603586"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ac772bd1af"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/11`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3fd8763e0247baadfc564ffa5aa432e4e4ea824ae729dfebe44c8ad4bad238ea -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key rotate"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "rotate_app_key",
  "path": "/v1/apps/{app}/keys/{key_id}/rotate",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
