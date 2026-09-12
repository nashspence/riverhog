# Operation parity: get_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-processing-claim:af4bfd805e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-aea5e1db0f"></a>
| Concern | Contract |
|---|---|
| <a id="s-d773f0ecac"></a>`application` | riverhog |
| <a id="s-0031ce127d"></a>`classification` | client-only-primitive |
| <a id="s-71b728e931"></a>`cli_commands` | [] |
| <a id="s-3feec0a67c"></a>`client` | ApiClient |
| <a id="s-a1b975fe20"></a>`method` | GET |
| <a id="s-b45dd13304"></a>`operation_id` | get_processing_claim |
| <a id="s-3f377994bc"></a>`path` | /v1/collection-processing-claims/{claim_id} |
| <a id="s-4af15564af"></a>`provider_evidence` | None |
| <a id="s-0d6a0c29eb"></a>`read_collection` | None |
| <a id="s-86104d70bb"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}](../http/get-v1-collection-processing-claims-claim-id.md)

## Governing policies

- <a id="pa-12e690f78d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-0a54141f21"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-54d99b95aa"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/26`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 87a90814e68a34cb73645f4d311d695c3ec17bda9824f1dd0c2ae0e58532727f -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_processing_claim",
  "path": "/v1/collection-processing-claims/{claim_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
