# Operation parity: plan_archive_copy_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-plan-archive-copy-retirement:c370035def -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e25774aeb7"></a>
| Concern | Contract |
|---|---|
| <a id="s-ba58b18c5e"></a>`application` | riverhog |
| <a id="s-9fa991d2d5"></a>`classification` | human-cli+json |
| <a id="s-09786e7372"></a>`cli_commands` | ["archive retire"] |
| <a id="s-c25d026ee0"></a>`client` | ApiClient |
| <a id="s-a2f8c0ef92"></a>`method` | POST |
| <a id="s-b7a60a1a13"></a>`operation_id` | plan_archive_copy_retirement |
| <a id="s-51113f99a4"></a>`path` | /v1/archive/copies/retirement-plan |
| <a id="s-923cd83cd6"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-81f91038d9"></a>`read_collection` | None |
| <a id="s-a344c8ab20"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/archive/copies/retirement-plan](../http/post-v1-archive-copies-retirement-plan.md)
- [piggity archive retire](../../piggity/cli/piggity-archive-retire.md)

## Governing policies

- <a id="pa-ba528cbf3b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-901a10fcfe"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-6ce16c172e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/15`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5140b4139f0920dc71be4e41a6dcda6ad290e8defcca0d375136a4342ebab44d -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive retire"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "plan_archive_copy_retirement",
  "path": "/v1/archive/copies/retirement-plan",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
