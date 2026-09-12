# Operation parity: get_archive_copy_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-archive-copy-job:6b18b3794b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ed953e19c166"></a>
| Concern | Contract |
|---|---|
| <a id="s-4ad55f906630"></a>`application` | riverhog |
| <a id="s-16ded74fc53e"></a>`classification` | human-cli+json |
| <a id="s-352e3de0d592"></a>`cli_commands` | ["archive copy show", "archive copy watch"] |
| <a id="s-2a1f5ae3b805"></a>`client` | ApiClient |
| <a id="s-7f32e9cdc601"></a>`method` | GET |
| <a id="s-5e042f68ab86"></a>`operation_id` | get_archive_copy_job |
| <a id="s-fcb545096336"></a>`path` | /v1/archive/copies/{collection_id}/{destination_store} |
| <a id="s-c88844231793"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-73ba93ee993e"></a>`read_collection` | None |
| <a id="s-6489f6bf0b33"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/copies/{collection_id}/{destination_store}](../http/get-v1-archive-copies-collection-id-destination-store.md)
- [piggity archive copy show](../../piggity/cli/piggity-archive-copy-show.md)
- [piggity archive copy watch](../../piggity/cli/piggity-archive-copy-watch.md)

## Governing policies

- <a id="pa-6d69a6c5f56e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-702c15069294"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-cb7a702db7f6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/17`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74e8a128af8f5b244f14163eb363c7b0a2171ee853fbda6236776af5e061d260 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive copy show",
    "archive copy watch"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_archive_copy_job",
  "path": "/v1/archive/copies/{collection_id}/{destination_store}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
