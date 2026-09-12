# Operation parity: get_download_quota

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-download-quota:f146167eeb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `download-quota` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["app key quota show"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_download_quota |
| `path` | /v1/download-quota |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/download-quota](../http/get-v1-download-quota.md)
- [piggity app key quota show](../../piggity/cli/piggity-app-key-quota-show.md)

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
