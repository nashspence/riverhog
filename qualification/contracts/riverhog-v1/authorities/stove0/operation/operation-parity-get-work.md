# Operation parity: get_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-work:eba7ff859e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [work](families/work/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-2dd3da0550"></a>
| Concern | Contract |
|---|---|
| <a id="s-006136492f"></a>`application` | stove0 |
| <a id="s-f6a621ba16"></a>`classification` | human-cli+json |
| <a id="s-6450dc4339"></a>`cli_commands` | ["work show"] |
| <a id="s-339aea867f"></a>`client` | Stove0ApiClient |
| <a id="s-cb5b71e96a"></a>`method` | GET |
| <a id="s-7278692953"></a>`operation_id` | get_work |
| <a id="s-c742dc63b0"></a>`path` | /v1/work/{work_id} |
| <a id="s-d0dde15f0a"></a>`provider_evidence` | None |
| <a id="s-8950ab00f0"></a>`read_collection` | None |
| <a id="s-2145ce1573"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/work/{work_id}](../http/get-v1-work-work-id.md)
- [stove0 work show](../cli/stove0-work-show.md)

## Governing policies

- <a id="pa-bc6cecf136"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-4d2a56438c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-0fce71da95"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/141`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a061abb3a64732deb3c022132a74f84feba5091b6d5d094ba64e30be352cc140 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work show"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "get_work",
  "path": "/v1/work/{work_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
