# Operation parity: list_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-processing-claim-dispositions:b4c2abe4af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-776bf7c73426"></a>
| Concern | Contract |
|---|---|
| <a id="s-c839122694e9"></a>`application` | riverhog |
| <a id="s-a42cb6e9e396"></a>`classification` | client-only-primitive |
| <a id="s-98357994229c"></a>`cli_commands` | [] |
| <a id="s-a72c705d9350"></a>`client` | ApiClient |
| <a id="s-812fb8038a1d"></a>`method` | GET |
| <a id="s-bb4da007d5a9"></a>`operation_id` | list_processing_claim_dispositions |
| <a id="s-6b61c8221980"></a>`path` | /v1/collection-processing-claims/{claim_id}/derivation/dispositions |
| <a id="s-2a17ce8cffa8"></a>`provider_evidence` | None |
| <a id="s-da8da5528e17"></a>`read_collection` | {"authority": "processing-claim-dispositions", "authority_parameter": "authority_sha256", "cursor_parameter": "start_ordinal", "fixed_limit": 128, "kind": "exact-authority-page"} |
| <a id="s-abff2ac10923"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/derivation/dispositions](../http/get-v1-collection-processing-claims-claim-id-derivation-dispositions.md)

## Governing policies

- <a id="pa-9da00e91c720"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-6f3f8aca0e16"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-409903080edb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/32`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c0d362770b39c3da3012a18780e936fa9281df6495e90aff5254b178ef9f7a7 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_processing_claim_dispositions",
  "path": "/v1/collection-processing-claims/{claim_id}/derivation/dispositions",
  "provider_evidence": null,
  "read_collection": {
    "authority": "processing-claim-dispositions",
    "authority_parameter": "authority_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```
