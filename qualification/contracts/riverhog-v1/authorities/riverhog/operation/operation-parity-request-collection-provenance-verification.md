# Operation parity: request_collection_provenance_verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-request-collection-prove-38819872a8:08264aff28 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4cb62082a58a"></a>
| Concern | Contract |
|---|---|
| <a id="s-bcd465337141"></a>`application` | riverhog |
| <a id="s-a5f1a955149e"></a>`classification` | human-cli+json |
| <a id="s-bd26ceef8c7f"></a>`cli_commands` | ["collection provenance verify"] |
| <a id="s-42058085ecb8"></a>`client` | ApiClient |
| <a id="s-b98d52712d6b"></a>`method` | POST |
| <a id="s-e553101e6447"></a>`operation_id` | request_collection_provenance_verification |
| <a id="s-c4bbc8fb780c"></a>`path` | /v1/collections/{collection_id}/provenance/verification |
| <a id="s-ce715002d097"></a>`provider_evidence` | None |
| <a id="s-b36ae6b12a56"></a>`read_collection` | None |
| <a id="s-bb861c122fc9"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collections/{collection_id}/provenance/verification](../http/post-v1-collections-collection-id-provenance-verification.md)
- [piggity collection provenance verify](../../piggity/cli/piggity-collection-provenance-verify.md)

## Governing policies

- <a id="pa-457a8fbf4b61"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-737252359f20"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-4edb781a5ecc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/85`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc8ebcc2d2b90338856ba0d924801d0a9a8a926fcf94eb12ce30e07ecea7f9a5 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance verify"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "request_collection_provenance_verification",
  "path": "/v1/collections/{collection_id}/provenance/verification",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
