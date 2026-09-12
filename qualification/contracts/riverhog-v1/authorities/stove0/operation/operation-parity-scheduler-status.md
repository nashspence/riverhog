# Operation parity: scheduler_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-scheduler-status:f8b1a09d22 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [admin](families/admin/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-84a71493df"></a>
| Concern | Contract |
|---|---|
| <a id="s-7121d27e7d"></a>`application` | stove0 |
| <a id="s-6ecb5d7853"></a>`classification` | human-cli+json |
| <a id="s-227fdb2215"></a>`cli_commands` | ["scheduler status"] |
| <a id="s-eb49d68267"></a>`client` | Stove0ApiClient |
| <a id="s-1ba54dfa9e"></a>`method` | GET |
| <a id="s-fdff6b0a6e"></a>`operation_id` | scheduler_status |
| <a id="s-274741c878"></a>`path` | /v1/admin/scheduler |
| <a id="s-102c0acc6e"></a>`provider_evidence` | None |
| <a id="s-c6f22d85e6"></a>`read_collection` | None |
| <a id="s-f7af1fae51"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/admin/scheduler](../http/get-v1-admin-scheduler.md)
- [stove0-client scheduler status](../../stove0-client/cli/stove0-client-scheduler-status.md)

## Governing policies

- <a id="pa-92e670aa22"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-913f5b7b41"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-20be1086bb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/116`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae0201cec0085f087322403983e0a8d2926ee73c04fd8dc18356d4d2e7dc2b31 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "scheduler status"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "scheduler_status",
  "path": "/v1/admin/scheduler",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
