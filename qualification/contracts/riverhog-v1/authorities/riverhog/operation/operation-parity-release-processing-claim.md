# Operation parity: release_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-release-processing-claim:2be9269abf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-2afb393536"></a>
| Concern | Contract |
|---|---|
| <a id="s-8b3388f9bc"></a>`application` | riverhog |
| <a id="s-664985be67"></a>`classification` | client-only-primitive |
| <a id="s-8a9779237f"></a>`cli_commands` | [] |
| <a id="s-11cc09eeca"></a>`client` | ApiClient |
| <a id="s-9a88cb0dfb"></a>`method` | POST |
| <a id="s-14c4043bd6"></a>`operation_id` | release_processing_claim |
| <a id="s-02e73d57eb"></a>`path` | /v1/collection-processing-claims/{claim_id}/release |
| <a id="s-d2574dbf29"></a>`provider_evidence` | None |
| <a id="s-a53ad53026"></a>`read_collection` | None |
| <a id="s-cee250c52d"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/release](../http/post-v1-collection-processing-claims-claim-id-release.md)

## Governing policies

- <a id="pa-b48417305a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-b58ed64c9c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-af25cffc90"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/46`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b8df9cf69c16d7865bbb95d8a07128adc28dcf7edba247d43fd47bcc2ef628dd -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "release_processing_claim",
  "path": "/v1/collection-processing-claims/{claim_id}/release",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
