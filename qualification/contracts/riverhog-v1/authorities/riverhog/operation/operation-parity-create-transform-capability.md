# Operation parity: create_transform_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-transform-capability:22713ab22e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-549c82bba035"></a>
| Concern | Contract |
|---|---|
| <a id="s-fb47aeeda41f"></a>`application` | riverhog |
| <a id="s-4418bd279a13"></a>`classification` | client-only-primitive |
| <a id="s-426d08c51cd3"></a>`cli_commands` | [] |
| <a id="s-e1cc29e4a1d2"></a>`client` | ApiClient |
| <a id="s-f13f5f7872b6"></a>`method` | POST |
| <a id="s-43515debc0f3"></a>`operation_id` | create_transform_capability |
| <a id="s-8c4d300300bc"></a>`path` | /v1/collection-processing-claims/{claim_id}/capabilities |
| <a id="s-408fe2bfa1e7"></a>`provider_evidence` | None |
| <a id="s-408fa6a4efef"></a>`read_collection` | None |
| <a id="s-63300ce6af92"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/capabilities](../http/post-v1-collection-processing-claims-claim-id-capabilities.md)

## Governing policies

- <a id="pa-988d591e836e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-040ece4e6b35"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-80bb2cb0946d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/28`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 195bd47b856a6e07f74019df60f9b66b7e7ad8d2708b1bf706994179f02c1ded -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_transform_capability",
  "path": "/v1/collection-processing-claims/{claim_id}/capabilities",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
