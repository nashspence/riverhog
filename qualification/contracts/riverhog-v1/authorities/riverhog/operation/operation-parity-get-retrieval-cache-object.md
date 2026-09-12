# Operation parity: get_retrieval_cache_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-retrieval-cache-object:001ab51ffb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-cache](families/retrieval-cache/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-be48d7551755"></a>
| Concern | Contract |
|---|---|
| <a id="s-c759b9262d8b"></a>`application` | riverhog |
| <a id="s-5966aad44722"></a>`classification` | human-cli+json |
| <a id="s-bdcdbfe762ae"></a>`cli_commands` | ["retrieval cache show"] |
| <a id="s-036494715b49"></a>`client` | ApiClient |
| <a id="s-fd713adbdd82"></a>`method` | GET |
| <a id="s-345c50c00c57"></a>`operation_id` | get_retrieval_cache_object |
| <a id="s-0892a0d0f4d3"></a>`path` | /v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id} |
| <a id="s-74f22676fb53"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-39564fbc4e65"></a>`read_collection` | None |
| <a id="s-f764ab57f3da"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}](../http/get-v1-retrieval-cache-objects-collection-id-source-store-object-id.md)
- [piggity retrieval cache show](../../piggity/cli/piggity-retrieval-cache-show.md)

## Governing policies

- <a id="pa-5f631922235c"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-ee14c348647f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-af06c3fd9f9a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/95`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 80bb7a9afd828c148050dd5685a715e8cfcfe32e6ada8683bbeabc0d0fd5281a -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "retrieval cache show"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_retrieval_cache_object",
  "path": "/v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
