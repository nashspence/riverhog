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

<a id="s-15bac6a5a1e1"></a>
| Concern | Contract |
|---|---|
| <a id="s-fef82303d616"></a>`application` | riverhog |
| <a id="s-5f868f770855"></a>`classification` | human-cli+json |
| <a id="s-da1ef93a1212"></a>`cli_commands` | ["app key rotate"] |
| <a id="s-2869f1b458ba"></a>`client` | ApiClient |
| <a id="s-977bdebe402c"></a>`method` | POST |
| <a id="s-308b380813bf"></a>`operation_id` | rotate_app_key |
| <a id="s-614d6a78b1f0"></a>`path` | /v1/apps/{app}/keys/{key_id}/rotate |
| <a id="s-747bbd16fd0b"></a>`provider_evidence` | None |
| <a id="s-bcb0108e1585"></a>`read_collection` | None |
| <a id="s-bb73251e2227"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/rotate](../http/post-v1-apps-app-keys-key-id-rotate.md)
- [piggity app key rotate](../../piggity/cli/piggity-app-key-rotate.md)

## Governing policies

- <a id="pa-6f629777cbf9"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-7b9f60358656"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-ac772bd1af76"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
