# Operation parity: rebaseline_admission_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-rebaseline-admission-policy:297bda9117 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `admission-policies` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["admission policy rebaseline"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | rebaseline_admission_policy |
| `path` | /v1/admission-policies/{policy_id}:rebaseline |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/admission-policies/{policy_id}:rebaseline](../http/post-v1-admission-policies-policy-id-rebaseline.md)
- [stove0 admission policy rebaseline](../cli/stove0-admission-policy-rebaseline.md)

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

- `/external_contract/operations/120`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a55800a0a74eff6cf05d00938b4b96dcfecf31063a50fa752cf956ef945de65c -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "admission policy rebaseline"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "rebaseline_admission_policy",
  "path": "/v1/admission-policies/{policy_id}:rebaseline",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
