# PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-upload-finalize-poll-seconds:2b47f46a96 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

| Field | Shape |
|---|---|
| `consumers` | ["piggity"] |
| `name` | "PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS" |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |

## Governing policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS` — `configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/7`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8fe9cc66b3ddf76f9a71f4d2f1af74152b2d746b6188cdd977d31202fd1f86b -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS"
}
```
