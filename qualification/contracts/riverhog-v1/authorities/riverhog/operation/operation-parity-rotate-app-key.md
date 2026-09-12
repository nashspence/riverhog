# Operation parity: rotate_app_key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-rotate-app-key:d175804ecd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `apps` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["app key rotate"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | rotate_app_key |
| `path` | /v1/apps/{app}/keys/{key_id}/rotate |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/rotate](../http/post-v1-apps-app-keys-key-id-rotate.md)
- [piggity app key rotate](../../piggity/cli/piggity-app-key-rotate.md)

## Governing policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`

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
