# Operation parity: record_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-record-processing-claim-2a587c911d:c4f5a56f18 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-60c49fc60c"></a>
| Concern | Contract |
|---|---|
| <a id="s-ecb821bb09"></a>`application` | riverhog |
| <a id="s-5357ad7836"></a>`classification` | client-only-primitive |
| <a id="s-6f12d0555e"></a>`cli_commands` | [] |
| <a id="s-ec1bcdc870"></a>`client` | ApiClient |
| <a id="s-c105ea94a2"></a>`method` | PUT |
| <a id="s-1291461410"></a>`operation_id` | record_processing_claim_dispositions |
| <a id="s-e9e304f51d"></a>`path` | /v1/collection-processing-claims/{claim_id}/derivation/dispositions |
| <a id="s-d04e6c2dcd"></a>`provider_evidence` | None |
| <a id="s-587344be84"></a>`read_collection` | None |
| <a id="s-da44860ca4"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/derivation/dispositions](../http/put-v1-collection-processing-claims-claim-id-derivation-dispositions.md)

## Governing policies

- <a id="pa-4b1e67934b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-e7e9e61397"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-70fcc6963a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/33`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e9a940ae650b2d7df7e7735f9c2dbb62ef2f8a9898d96e5c9658ef1d0450266f -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "record_processing_claim_dispositions",
  "path": "/v1/collection-processing-claims/{claim_id}/derivation/dispositions",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
