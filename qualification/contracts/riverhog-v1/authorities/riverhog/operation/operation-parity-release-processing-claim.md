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

<a id="s-2afb39353607"></a>
| Concern | Contract |
|---|---|
| <a id="s-8b3388f9bcd7"></a>`application` | riverhog |
| <a id="s-664985be6732"></a>`classification` | client-only-primitive |
| <a id="s-8a9779237f9a"></a>`cli_commands` | [] |
| <a id="s-11cc09eeca3c"></a>`client` | ApiClient |
| <a id="s-9a88cb0dfb80"></a>`method` | POST |
| <a id="s-14c4043bd68c"></a>`operation_id` | release_processing_claim |
| <a id="s-02e73d57ebb9"></a>`path` | /v1/collection-processing-claims/{claim_id}/release |
| <a id="s-d2574dbf29be"></a>`provider_evidence` | None |
| <a id="s-a53ad53026d4"></a>`read_collection` | None |
| <a id="s-cee250c52d4e"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/release](../http/post-v1-collection-processing-claims-claim-id-release.md)

## Governing policies

- <a id="pa-b48417305ac6"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-b58ed64c9ca8"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-af25cffc9000"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
