# Operation parity: get_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection:b0eb906e34 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-093bf8d8cb"></a>
| Concern | Contract |
|---|---|
| <a id="s-57b34a7ff4"></a>`application` | riverhog |
| <a id="s-a936b593aa"></a>`classification` | human-cli+json |
| <a id="s-334ca61e99"></a>`cli_commands` | ["collection describe", "collection show", "collection tag add", "collection tag contains", "collection tag list", "collection tag remove", "local add", "local repair", "local sync"] |
| <a id="s-590ac3387c"></a>`client` | ApiClient |
| <a id="s-a631327e05"></a>`method` | GET |
| <a id="s-01d12a850a"></a>`operation_id` | get_collection |
| <a id="s-7c8deefce6"></a>`path` | /v1/collections/{collection_id} |
| <a id="s-f21b19e12a"></a>`provider_evidence` | None |
| <a id="s-1cec0797bb"></a>`read_collection` | None |
| <a id="s-1700f18353"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../http/get-v1-collections-collection-id.md)
- [piggity collection describe](../../piggity/cli/piggity-collection-describe.md)
- [piggity collection show](../../piggity/cli/piggity-collection-show.md)
- [piggity collection tag add](../../piggity/cli/piggity-collection-tag-add.md)
- [piggity collection tag contains](../../piggity/cli/piggity-collection-tag-contains.md)
- [piggity collection tag list](../../piggity/cli/piggity-collection-tag-list.md)
- [piggity collection tag remove](../../piggity/cli/piggity-collection-tag-remove.md)
- [piggity local add](../../piggity/cli/piggity-local-add.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-c01a9feb48"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-de83567973"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-a4a396835d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/71`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d4e9aa9c662f3db700ab23d40bd878560fbaf6b88152acb244f566eaa1ab443 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection describe",
    "collection show",
    "collection tag add",
    "collection tag contains",
    "collection tag list",
    "collection tag remove",
    "local add",
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_collection",
  "path": "/v1/collections/{collection_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
