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

<a id="s-aea5e1db0f38"></a>
| Concern | Contract |
|---|---|
| <a id="s-d773f0ecac1c"></a>`application` | riverhog |
| <a id="s-0031ce127d42"></a>`classification` | client-only-primitive |
| <a id="s-71b728e93198"></a>`cli_commands` | [] |
| <a id="s-3feec0a67ca2"></a>`client` | ApiClient |
| <a id="s-a1b975fe2059"></a>`method` | GET |
| <a id="s-b45dd133049f"></a>`operation_id` | get_processing_claim |
| <a id="s-3f377994bcc4"></a>`path` | /v1/collection-processing-claims/{claim_id} |
| <a id="s-4af15564afd7"></a>`provider_evidence` | None |
| <a id="s-0d6a0c29eb2e"></a>`read_collection` | None |
| <a id="s-86104d70bb52"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}](../http/get-v1-collection-processing-claims-claim-id.md)

## Governing policies

- <a id="pa-12e690f78d0e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-0a54141f2110"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-54d99b95aa00"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
