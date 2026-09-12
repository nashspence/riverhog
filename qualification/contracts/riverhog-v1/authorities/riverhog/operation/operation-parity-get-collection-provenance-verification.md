# Operation parity: get_collection_provenance_verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection-provenanc-292ae4cdc5:b917ccacc1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-813c7c6b43"></a>
| Concern | Contract |
|---|---|
| <a id="s-320d92fb91"></a>`application` | riverhog |
| <a id="s-f08daec3fe"></a>`classification` | human-cli+json |
| <a id="s-cd3d80ad5d"></a>`cli_commands` | ["collection provenance verification-show", "collection provenance verify"] |
| <a id="s-4dcf68fbc4"></a>`client` | ApiClient |
| <a id="s-df8e94519c"></a>`method` | GET |
| <a id="s-7fd78c3a31"></a>`operation_id` | get_collection_provenance_verification |
| <a id="s-e3958f33fc"></a>`path` | /v1/collections/{collection_id}/provenance/verification |
| <a id="s-4e108d43e8"></a>`provider_evidence` | None |
| <a id="s-470e0cfcf5"></a>`read_collection` | None |
| <a id="s-afeadedd04"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/verification](../http/get-v1-collections-collection-id-provenance-verification.md)
- [piggity collection provenance verification-show](../../piggity/cli/piggity-collection-provenance-verification-show.md)
- [piggity collection provenance verify](../../piggity/cli/piggity-collection-provenance-verify.md)

## Governing policies

- <a id="pa-3ab47c62d0"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5d9718ffef"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-9216a9fd6e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/84`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b588b438e2903cc45f0207d3566a499f32975bcc5fb4c335ff0b9ff3f6d167b6 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance verification-show",
    "collection provenance verify"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_collection_provenance_verification",
  "path": "/v1/collections/{collection_id}/provenance/verification",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
