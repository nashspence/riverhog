# Operation parity: rebaseline_admission_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-rebaseline-admission-policy:297bda9117 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [admission-policies](families/admission-policies/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-284d4e505f"></a>
| Concern | Contract |
|---|---|
| <a id="s-04ca11646e"></a>`application` | stove0 |
| <a id="s-799de24f4f"></a>`classification` | human-cli+json |
| <a id="s-32768f0e75"></a>`cli_commands` | ["admission policy rebaseline"] |
| <a id="s-234f155ba4"></a>`client` | Stove0ApiClient |
| <a id="s-4bd8921ed1"></a>`method` | POST |
| <a id="s-91c7977867"></a>`operation_id` | rebaseline_admission_policy |
| <a id="s-5399bcaf79"></a>`path` | /v1/admission-policies/{policy_id}:rebaseline |
| <a id="s-15a86a95cd"></a>`provider_evidence` | None |
| <a id="s-d3cdada67b"></a>`read_collection` | None |
| <a id="s-411f929b67"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/admission-policies/{policy_id}:rebaseline](../http/post-v1-admission-policies-policy-id-rebaseline.md)
- [stove0 admission policy rebaseline](../cli/stove0-admission-policy-rebaseline.md)

## Governing policies

- <a id="pa-739261b1b0"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-12967209df"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-81849b9f11"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

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
