# Operation parity: get_retrieval_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-retrieval-plan:0730cea2c7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-plans](families/retrieval-plans/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-58091051d8"></a>
| Concern | Contract |
|---|---|
| <a id="s-326c527b90"></a>`application` | riverhog |
| <a id="s-cb27557fdc"></a>`classification` | client-only-primitive |
| <a id="s-68a1ea1912"></a>`cli_commands` | [] |
| <a id="s-96de5de997"></a>`client` | ApiClient |
| <a id="s-32f6f6de33"></a>`method` | GET |
| <a id="s-59049916f4"></a>`operation_id` | get_retrieval_plan |
| <a id="s-8e44430c9c"></a>`path` | /v1/retrieval-plans/{plan_id} |
| <a id="s-0a21946d17"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-63cb30de18"></a>`read_collection` | None |
| <a id="s-6e32262968"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-plans/{plan_id}](../http/get-v1-retrieval-plans-plan-id.md)

## Governing policies

- <a id="pa-30fc52f86f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d520aaed4e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-12b23e8176"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/104`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e32bfe94ff0f9dc1e23e876a0ca1f9f6121ea0f2e5d035ad7b41a1a44db8cad8 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_retrieval_plan",
  "path": "/v1/retrieval-plans/{plan_id}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
