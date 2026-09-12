# Operation parity: get_artifact_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-artifact-selection:cde72fc375 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `artifact-selections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/123`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/artifact-selections/{selection_sha256}](../http/get-v1-artifact-selections-selection-sha256.md)
- [stove0 selection show](../cli/stove0-selection-show.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["selection show"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | get_artifact_selection |
| `path` | /v1/artifact-selections/{selection_sha256} |
| `provider_evidence` | None |
| `read_collection` | {"authority": "artifact-selection", "authority_parameter": "selection_sha256", "cursor_parameter": "continuation", "fixed_limit": 256, "kind": "exact-authority-page"} |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fd9fa51297007777050cab06b4db0978155bc657f20a0f2047a36fe8f63f863 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "selection show"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "get_artifact_selection",
  "path": "/v1/artifact-selections/{selection_sha256}",
  "provider_evidence": null,
  "read_collection": {
    "authority": "artifact-selection",
    "authority_parameter": "selection_sha256",
    "cursor_parameter": "continuation",
    "fixed_limit": 256,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```
