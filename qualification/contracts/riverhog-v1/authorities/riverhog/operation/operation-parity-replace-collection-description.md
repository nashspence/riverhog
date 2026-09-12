# Operation parity: replace_collection_description

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-replace-collection-description:1d61b9e54f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f6a67a3bd6b2"></a>
| Concern | Contract |
|---|---|
| <a id="s-00177a701c61"></a>`application` | riverhog |
| <a id="s-6f33e2b2c3d6"></a>`classification` | human-cli+json |
| <a id="s-8bd17781820b"></a>`cli_commands` | ["collection describe"] |
| <a id="s-7822ccf723b0"></a>`client` | ApiClient |
| <a id="s-40e924c85e8a"></a>`method` | PUT |
| <a id="s-1241097e716b"></a>`operation_id` | replace_collection_description |
| <a id="s-72cbef1be235"></a>`path` | /v1/collections/{collection_id}/description |
| <a id="s-349efef4aaba"></a>`provider_evidence` | None |
| <a id="s-8d2e38b23f16"></a>`read_collection` | None |
| <a id="s-d9d449f08612"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [PUT /v1/collections/{collection_id}/description](../http/put-v1-collections-collection-id-description.md)
- [piggity collection describe](../../piggity/cli/piggity-collection-describe.md)

## Governing policies

- <a id="pa-1a58511d5824"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-da1da282548c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-c82eed2fe419"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/76`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dfb4f8db18d330335224d579c3b794b62d8331c2c992379113ac55bc32cc515f -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection describe"
  ],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "replace_collection_description",
  "path": "/v1/collections/{collection_id}/description",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
