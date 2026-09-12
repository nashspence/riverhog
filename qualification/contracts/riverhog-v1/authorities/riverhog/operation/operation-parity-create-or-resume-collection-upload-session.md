# Operation parity: create_or_resume_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-or-resume-collect-e3d1a894e1:90d65f8839 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-730856f554"></a>
| Concern | Contract |
|---|---|
| <a id="s-f031c97108"></a>`application` | riverhog |
| <a id="s-e180e23260"></a>`classification` | client-only-primitive |
| <a id="s-fab54fc544"></a>`cli_commands` | ["collection upload start"] |
| <a id="s-4b2f40d56c"></a>`client` | ApiClient |
| <a id="s-882294b4f1"></a>`method` | POST |
| <a id="s-0bad6e445f"></a>`operation_id` | create_or_resume_collection_upload_session |
| <a id="s-46d374ef79"></a>`path` | /v1/collection-upload-sessions |
| <a id="s-391f7b9338"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-58622de0aa"></a>`read_collection` | None |
| <a id="s-76988ec547"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions](../http/post-v1-collection-upload-sessions.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Governing policies

- <a id="pa-c6f6ad7f85"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-ad9bd274c3"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-7ca2dac2b8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/52`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1b4e5ecfe060eb6e0885995677578313ed7b4d1b0c721083a36c560859f1466b -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_or_resume_collection_upload_session",
  "path": "/v1/collection-upload-sessions",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
