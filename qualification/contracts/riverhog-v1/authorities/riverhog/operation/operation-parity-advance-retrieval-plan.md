# Operation parity: advance_retrieval_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-advance-retrieval-plan:091592cb5c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-plans](families/retrieval-plans/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e93ec8dcceb2"></a>
| Concern | Contract |
|---|---|
| <a id="s-99ab97e23112"></a>`application` | riverhog |
| <a id="s-04c916c5406e"></a>`classification` | client-only-primitive |
| <a id="s-761599a9c4c0"></a>`cli_commands` | ["local repair", "local sync"] |
| <a id="s-c3440b836c7e"></a>`client` | ApiClient |
| <a id="s-20310a6bcae5"></a>`method` | POST |
| <a id="s-940d4db1ef36"></a>`operation_id` | advance_retrieval_plan |
| <a id="s-bb3705fd7d17"></a>`path` | /v1/retrieval-plans/{plan_id}/advance |
| <a id="s-36599c19d6b9"></a>`provider_evidence` | None |
| <a id="s-0d1732b84207"></a>`read_collection` | None |
| <a id="s-7f5d633cadb5"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/retrieval-plans/{plan_id}/advance](../http/post-v1-retrieval-plans-plan-id-advance.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-4b357af86525"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-080467c6c7d3"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-a55f5ed94c06"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/105`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1399a10588f0112e78b25e17e909cf500b6c7b022cc32996dab9267cc3d3662 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "advance_retrieval_plan",
  "path": "/v1/retrieval-plans/{plan_id}/advance",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
