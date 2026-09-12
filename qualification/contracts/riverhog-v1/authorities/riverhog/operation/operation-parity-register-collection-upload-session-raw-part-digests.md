# Operation parity: register_collection_upload_session_raw_part_digests

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-register-collection-uplo-6d707f49d9:4b99b87ddb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1727fbdf2557"></a>
| Concern | Contract |
|---|---|
| <a id="s-fe116469527c"></a>`application` | riverhog |
| <a id="s-404e388b3360"></a>`classification` | client-only-primitive |
| <a id="s-7758c55c7e8f"></a>`cli_commands` | ["collection upload start"] |
| <a id="s-d20d76364e52"></a>`client` | ApiClient |
| <a id="s-a5ff3a5852ae"></a>`method` | POST |
| <a id="s-e051c8e583b7"></a>`operation_id` | register_collection_upload_session_raw_part_digests |
| <a id="s-4d51d67b7a1b"></a>`path` | /v1/collection-upload-sessions/{collection_id}/raw-part-digests |
| <a id="s-286a438ce16b"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-8d3d9a2093f3"></a>`read_collection` | None |
| <a id="s-952b23bd7d37"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/raw-part-digests](../http/post-v1-collection-upload-sessions-collection-id-raw-part-digests.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Governing policies

- <a id="pa-63c333da6f33"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-4582dc64066b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-66c290c2cc8e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/65`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc08b386800cd3843d23ac80251ca562cf39b003222d00e3af2ca2e8b373d6e1 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "register_collection_upload_session_raw_part_digests",
  "path": "/v1/collection-upload-sessions/{collection_id}/raw-part-digests",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
