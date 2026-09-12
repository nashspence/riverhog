# Operation parity: cancel_collection_provenance_verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-cancel-collection-proven-9adb6dc042:e3e154f60d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b23af64d7a"></a>
| Concern | Contract |
|---|---|
| <a id="s-f15d689464"></a>`application` | riverhog |
| <a id="s-380a886b54"></a>`classification` | human-cli+json |
| <a id="s-8dfa87ade7"></a>`cli_commands` | ["collection provenance verification-cancel"] |
| <a id="s-b9c350b42c"></a>`client` | ApiClient |
| <a id="s-dd6d867640"></a>`method` | DELETE |
| <a id="s-d7e546e72d"></a>`operation_id` | cancel_collection_provenance_verification |
| <a id="s-36fb9afb89"></a>`path` | /v1/collections/{collection_id}/provenance/verification |
| <a id="s-290c97a32d"></a>`provider_evidence` | None |
| <a id="s-0c149b933f"></a>`read_collection` | None |
| <a id="s-f1a631dc43"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [DELETE /v1/collections/{collection_id}/provenance/verification](../http/delete-v1-collections-collection-id-provenance-verification.md)
- [piggity collection provenance verification-cancel](../../piggity/cli/piggity-collection-provenance-verification-cancel.md)

## Governing policies

- <a id="pa-9b841e7414"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-bcfade18b5"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-4ac128711c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/83`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 224aa036ea4bd10ed5d542d08485c0a864b7859e6227caddf17e2694e2d14664 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance verification-cancel"
  ],
  "client": "ApiClient",
  "method": "DELETE",
  "operation_id": "cancel_collection_provenance_verification",
  "path": "/v1/collections/{collection_id}/provenance/verification",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
