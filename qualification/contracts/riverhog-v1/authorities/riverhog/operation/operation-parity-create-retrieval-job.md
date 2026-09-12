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

<a id="s-92d654f925dd"></a>
| Concern | Contract |
|---|---|
| <a id="s-3074315543cf"></a>`application` | riverhog |
| <a id="s-a74549b46d65"></a>`classification` | client-only-primitive |
| <a id="s-2d137f07e500"></a>`cli_commands` | ["local repair", "local sync"] |
| <a id="s-be70f317275b"></a>`client` | ApiClient |
| <a id="s-b6a95ea47247"></a>`method` | POST |
| <a id="s-921502003468"></a>`operation_id` | create_retrieval_job |
| <a id="s-37414df3ca2a"></a>`path` | /v1/retrieval-jobs |
| <a id="s-dd4502267d19"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-711346b70bef"></a>`read_collection` | None |
| <a id="s-690bc00750f9"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/retrieval-jobs](../http/post-v1-retrieval-jobs.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-947939a67033"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-f8b953de06b1"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-495938c764f1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
