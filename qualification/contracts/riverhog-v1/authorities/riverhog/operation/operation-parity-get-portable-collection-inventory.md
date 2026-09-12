# Operation parity: get_portable_collection_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-portable-collection-inventory:dfff9d2458 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [catalog](families/catalog/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-27e1c52db8"></a>
| Concern | Contract |
|---|---|
| <a id="s-8aed82ec93"></a>`application` | riverhog |
| <a id="s-d53fbd3ad1"></a>`classification` | standard-tool/protocol |
| <a id="s-59dd3f85c1"></a>`cli_commands` | ["local add", "local repair", "local sync"] |
| <a id="s-7bc8ef3b60"></a>`client` | ApiClient |
| <a id="s-68e838cd46"></a>`method` | GET |
| <a id="s-467f499d96"></a>`operation_id` | get_portable_collection_inventory |
| <a id="s-c1d004db00"></a>`path` | /v1/catalog/collections/{collection_id}/inventory |
| <a id="s-c63115934b"></a>`provider_evidence` | None |
| <a id="s-47445f4624"></a>`read_collection` | {"authority": "portable-collection-inventory", "cursor_parameter": "cursor", "kind": "exact-set-page", "limit_parameter": "limit", "validator_header": "If-Match"} |
| <a id="s-24a66b8603"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog/collections/{collection_id}/inventory](../http/get-v1-catalog-collections-collection-id-inventory.md)
- [piggity local add](../../piggity/cli/piggity-local-add.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-aecd87aa60"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-368e71c79f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-70bc853284"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/23`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8213f8a842ae33333a64887e3b95137b436ef1bba715eadfaf52fea8b74928f4 -->

```json
{
  "application": "riverhog",
  "classification": "standard-tool/protocol",
  "cli_commands": [
    "local add",
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_portable_collection_inventory",
  "path": "/v1/catalog/collections/{collection_id}/inventory",
  "provider_evidence": null,
  "read_collection": {
    "authority": "portable-collection-inventory",
    "cursor_parameter": "cursor",
    "kind": "exact-set-page",
    "limit_parameter": "limit",
    "validator_header": "If-Match"
  },
  "response_authority": "canonical-document"
}
```
