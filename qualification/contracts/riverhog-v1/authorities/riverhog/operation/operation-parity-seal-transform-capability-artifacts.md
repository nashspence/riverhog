# Operation parity: seal_transform_capability_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-seal-transform-capability-artifacts:27118ccd60 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-74d7acdad3"></a>
| Concern | Contract |
|---|---|
| <a id="s-260efb4b6d"></a>`application` | riverhog |
| <a id="s-4b42a0a27d"></a>`classification` | client-only-primitive |
| <a id="s-6150c80161"></a>`cli_commands` | [] |
| <a id="s-72fa94dfdd"></a>`client` | ApiClient |
| <a id="s-8bbc53d439"></a>`method` | POST |
| <a id="s-b7fb23cb13"></a>`operation_id` | seal_transform_capability_artifacts |
| <a id="s-d508484666"></a>`path` | /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts/seal |
| <a id="s-b1f897affe"></a>`provider_evidence` | None |
| <a id="s-cb77bb6d9d"></a>`read_collection` | None |
| <a id="s-e673db522a"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts/seal](../http/post-v1-collection-processing-claims-claim-id-capabilities-capability-id-artifacts-seal.md)

## Governing policies

- <a id="pa-9c5b92c78c"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-bb352db6d1"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-5d4fb58f9b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/30`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb98443fad1c77f7c60f09f95731f0b07b18aa91cc087e1cc9854d1f82bb1a3f -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "seal_transform_capability_artifacts",
  "path": "/v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts/seal",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
