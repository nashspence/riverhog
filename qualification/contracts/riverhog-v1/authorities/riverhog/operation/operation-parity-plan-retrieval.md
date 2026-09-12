# Operation parity: plan_retrieval

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-plan-retrieval:cc435cff02 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-plans](families/retrieval-plans/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-19ed8fea11"></a>
| Concern | Contract |
|---|---|
| <a id="s-f923fea766"></a>`application` | riverhog |
| <a id="s-96b128c8b0"></a>`classification` | client-only-primitive |
| <a id="s-2ca582b5cb"></a>`cli_commands` | ["local repair", "local sync"] |
| <a id="s-a139d7ca42"></a>`client` | ApiClient |
| <a id="s-82dc6195e4"></a>`method` | POST |
| <a id="s-d22cb0b1a7"></a>`operation_id` | plan_retrieval |
| <a id="s-0ddcd0d351"></a>`path` | /v1/retrieval-plans |
| <a id="s-b6683e9d3f"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-de7b62d227"></a>`read_collection` | None |
| <a id="s-a93016fca4"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/retrieval-plans](../http/post-v1-retrieval-plans.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-bfabfe3ef5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-7b6829d6e3"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-4e9e509f15"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/103`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b80934f387cd71d129c2490029f8954c0e775ed265141d21458466f71039930e -->

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
  "operation_id": "plan_retrieval",
  "path": "/v1/retrieval-plans",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
