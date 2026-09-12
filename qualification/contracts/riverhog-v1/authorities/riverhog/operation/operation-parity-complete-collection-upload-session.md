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

<a id="s-2900186c5f"></a>
| Concern | Contract |
|---|---|
| <a id="s-8693c3022b"></a>`application` | riverhog |
| <a id="s-8258fe516f"></a>`classification` | client-only-primitive |
| <a id="s-5a69a97875"></a>`cli_commands` | ["collection upload start"] |
| <a id="s-625c1a3da7"></a>`client` | ApiClient |
| <a id="s-357cc8e713"></a>`method` | POST |
| <a id="s-d85f56213c"></a>`operation_id` | complete_collection_upload_session |
| <a id="s-443bfd1afc"></a>`path` | /v1/collection-upload-sessions/{collection_id}/complete |
| <a id="s-f96d87a111"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-87017fd831"></a>`read_collection` | None |
| <a id="s-c1d8a44f71"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/complete](../http/post-v1-collection-upload-sessions-collection-id-complete.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Governing policies

- <a id="pa-7c620dfc54"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-8369e7e5ef"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-07b0451c99"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

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
