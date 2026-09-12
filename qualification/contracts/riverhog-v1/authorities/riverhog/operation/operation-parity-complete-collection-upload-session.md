# Operation parity: complete_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-complete-collection-upload-session:e6ea8da52b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-2900186c5f76"></a>
| Concern | Contract |
|---|---|
| <a id="s-8693c3022bd9"></a>`application` | riverhog |
| <a id="s-8258fe516f76"></a>`classification` | client-only-primitive |
| <a id="s-5a69a97875e7"></a>`cli_commands` | ["collection upload start"] |
| <a id="s-625c1a3da741"></a>`client` | ApiClient |
| <a id="s-357cc8e713f3"></a>`method` | POST |
| <a id="s-d85f56213cd9"></a>`operation_id` | complete_collection_upload_session |
| <a id="s-443bfd1afcfb"></a>`path` | /v1/collection-upload-sessions/{collection_id}/complete |
| <a id="s-f96d87a1111a"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-87017fd83124"></a>`read_collection` | None |
| <a id="s-c1d8a44f71c4"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/complete](../http/post-v1-collection-upload-sessions-collection-id-complete.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Governing policies

- <a id="pa-7c620dfc54ab"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-8369e7e5ef22"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-07b0451c99bf"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/55`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73ed3b84d186ba2c91da879e0f6a73a63612295215ebe0b953b2badccc75df3e -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "complete_collection_upload_session",
  "path": "/v1/collection-upload-sessions/{collection_id}/complete",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
