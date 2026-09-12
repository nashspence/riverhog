# Operation parity: get_collection_file_provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection-file-provenance:42585e3bff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-dcba69f7bf"></a>
| Concern | Contract |
|---|---|
| <a id="s-5e611bfcfb"></a>`application` | riverhog |
| <a id="s-345f840320"></a>`classification` | human-cli+json |
| <a id="s-5ec63ae399"></a>`cli_commands` | ["collection provenance show"] |
| <a id="s-f9274f60c2"></a>`client` | ApiClient |
| <a id="s-70cd59482c"></a>`method` | GET |
| <a id="s-e5000fcacf"></a>`operation_id` | get_collection_file_provenance |
| <a id="s-fe941c39f7"></a>`path` | /v1/collections/{collection_id}/provenance/files/{path} |
| <a id="s-c97012c7f8"></a>`provider_evidence` | None |
| <a id="s-ac34cf866c"></a>`read_collection` | None |
| <a id="s-0795da67d0"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/files/{path}](../http/get-v1-collections-collection-id-provenance-files-path.md)
- [piggity collection provenance show](../../piggity/cli/piggity-collection-provenance-show.md)

## Governing policies

- <a id="pa-d4767e610a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-06d5fae799"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-923a76c042"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/78`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dab9ba3f33afa5f2615082519fd2a680c2b8b310099bf62942db8b1bcb71f108 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance show"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_collection_file_provenance",
  "path": "/v1/collections/{collection_id}/provenance/files/{path}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
