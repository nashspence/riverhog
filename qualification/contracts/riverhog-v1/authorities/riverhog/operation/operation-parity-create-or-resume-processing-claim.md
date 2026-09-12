# Operation parity: create_or_resume_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-or-resume-processing-claim:d312b0c923 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-192db325e5"></a>
| Concern | Contract |
|---|---|
| <a id="s-3768c43720"></a>`application` | riverhog |
| <a id="s-346680ab1f"></a>`classification` | client-only-primitive |
| <a id="s-2765ff57ba"></a>`cli_commands` | [] |
| <a id="s-f83c595b64"></a>`client` | ApiClient |
| <a id="s-b8aae60fad"></a>`method` | POST |
| <a id="s-9beb1eb0fc"></a>`operation_id` | create_or_resume_processing_claim |
| <a id="s-91809f8379"></a>`path` | /v1/collection-processing-claims |
| <a id="s-3d1b1f4167"></a>`provider_evidence` | None |
| <a id="s-390eda3eac"></a>`read_collection` | None |
| <a id="s-e56d26ad50"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims](../http/post-v1-collection-processing-claims.md)

## Governing policies

- <a id="pa-447c286688"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-2f3e3fb6d7"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-00bfd3c308"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/25`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4da9ae21164eb4f13e3bcb3ef9b0e63da266a3c480b4b6009c3c0fe74238cd92 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_or_resume_processing_claim",
  "path": "/v1/collection-processing-claims",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
