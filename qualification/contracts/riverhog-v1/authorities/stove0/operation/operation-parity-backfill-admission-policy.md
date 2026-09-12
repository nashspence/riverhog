# Operation parity: backfill_admission_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-backfill-admission-policy:efa04c17d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [admission-policies](families/admission-policies/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c728f819fb"></a>
| Concern | Contract |
|---|---|
| <a id="s-b34cda116e"></a>`application` | stove0 |
| <a id="s-439f312fb4"></a>`classification` | human-cli+json |
| <a id="s-ec194032fe"></a>`cli_commands` | ["admission policy backfill"] |
| <a id="s-4f32a70756"></a>`client` | Stove0ApiClient |
| <a id="s-34ec179d24"></a>`method` | POST |
| <a id="s-38fc0d83dc"></a>`operation_id` | backfill_admission_policy |
| <a id="s-e42431f2a3"></a>`path` | /v1/admission-policies/{policy_id}:backfill |
| <a id="s-5eb3216268"></a>`provider_evidence` | None |
| <a id="s-86b107aeed"></a>`read_collection` | None |
| <a id="s-cebb69abbb"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/admission-policies/{policy_id}:backfill](../http/post-v1-admission-policies-policy-id-backfill.md)
- [stove0 admission policy backfill](../cli/stove0-admission-policy-backfill.md)

## Governing policies

- <a id="pa-61e5189a26"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-20dc8ad377"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-2048f8671f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/119`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ec8d7c36da38a53f7db66c1a1c91b876aaad847f9b43337f0d94e44f87556e6 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "admission policy backfill"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "backfill_admission_policy",
  "path": "/v1/admission-policies/{policy_id}:backfill",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
