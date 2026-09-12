# Operation parity: create_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-retrieval-job:5c3dd69b44 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-jobs](families/retrieval-jobs/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-92d654f925"></a>
| Concern | Contract |
|---|---|
| <a id="s-3074315543"></a>`application` | riverhog |
| <a id="s-a74549b46d"></a>`classification` | client-only-primitive |
| <a id="s-2d137f07e5"></a>`cli_commands` | ["local repair", "local sync"] |
| <a id="s-be70f31727"></a>`client` | ApiClient |
| <a id="s-b6a95ea472"></a>`method` | POST |
| <a id="s-9215020034"></a>`operation_id` | create_retrieval_job |
| <a id="s-37414df3ca"></a>`path` | /v1/retrieval-jobs |
| <a id="s-dd4502267d"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-711346b70b"></a>`read_collection` | None |
| <a id="s-690bc00750"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/retrieval-jobs](../http/post-v1-retrieval-jobs.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-947939a670"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-f8b953de06"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-495938c764"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/96`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 03bf3338113e196bf724178532a6dd3c8565847fd5a023b49230d4d792ca5782 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_retrieval_job",
  "path": "/v1/retrieval-jobs",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
