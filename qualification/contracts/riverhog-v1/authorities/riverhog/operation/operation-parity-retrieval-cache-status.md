# Operation parity: retrieval_cache_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-retrieval-cache-status:e4ad867bbd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-cache](families/retrieval-cache/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d60f71c91fd7"></a>
| Concern | Contract |
|---|---|
| <a id="s-f0ce87f5b39e"></a>`application` | riverhog |
| <a id="s-86ddc0279430"></a>`classification` | human-cli+json |
| <a id="s-ffb849fc3681"></a>`cli_commands` | ["retrieval cache status"] |
| <a id="s-e4d97e2454ac"></a>`client` | ApiClient |
| <a id="s-82bf7bd1324e"></a>`method` | GET |
| <a id="s-271ebecc9b48"></a>`operation_id` | retrieval_cache_status |
| <a id="s-bf39ac4006dc"></a>`path` | /v1/retrieval-cache |
| <a id="s-c1557f150461"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-914416e0c15f"></a>`read_collection` | None |
| <a id="s-66bcf13a4c81"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-cache](../http/get-v1-retrieval-cache.md)
- [piggity retrieval cache status](../../piggity/cli/piggity-retrieval-cache-status.md)

## Governing policies

- <a id="pa-669b95ce18de"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-55239dc03e82"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-fd03291307de"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/93`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7fa39ad8f54924de3d6f307196a23dd7d8bb34c661dd64763376b26def7b16e5 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "retrieval cache status"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "retrieval_cache_status",
  "path": "/v1/retrieval-cache",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
