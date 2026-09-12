# Operation parity: get_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection-upload-session:0b596090fe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-490d8ea2f814"></a>
| Concern | Contract |
|---|---|
| <a id="s-ca111b2c2e48"></a>`application` | riverhog |
| <a id="s-023a556e60aa"></a>`classification` | human-cli+json |
| <a id="s-bd1d1e246a63"></a>`cli_commands` | ["collection upload show", "collection upload start", "collection upload watch"] |
| <a id="s-2339eaa4c071"></a>`client` | ApiClient |
| <a id="s-e3882cffb0d5"></a>`method` | GET |
| <a id="s-5a01ced4cfab"></a>`operation_id` | get_collection_upload_session |
| <a id="s-712eba82fd57"></a>`path` | /v1/collection-upload-sessions/{collection_id} |
| <a id="s-bebfec6db7dd"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-2eee58b34e87"></a>`read_collection` | None |
| <a id="s-ea96f981b59e"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}](../http/get-v1-collection-upload-sessions-collection-id.md)
- [piggity collection upload show](../../piggity/cli/piggity-collection-upload-show.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [piggity collection upload watch](../../piggity/cli/piggity-collection-upload-watch.md)

## Governing policies

- <a id="pa-6f05c349428f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-028fa985a106"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-6f020f879273"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/53`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0dd7f6275581bcc8379381c696ffd3ac7ffd74142f303050a46c92a6639bb9e -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection upload show",
    "collection upload start",
    "collection upload watch"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_collection_upload_session",
  "path": "/v1/collection-upload-sessions/{collection_id}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
