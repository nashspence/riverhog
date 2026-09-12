# Operation parity: list_lifecycle_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-lifecycle-events:d946aa0590 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [events](families/events/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-da9981e7c0"></a>
| Concern | Contract |
|---|---|
| <a id="s-b456656ecb"></a>`application` | riverhog |
| <a id="s-84ebd7f1d1"></a>`classification` | human-cli+json |
| <a id="s-cb54a42afc"></a>`cli_commands` | ["event list"] |
| <a id="s-e8d51acd36"></a>`client` | ApiClient |
| <a id="s-af74df6496"></a>`method` | GET |
| <a id="s-654befd326"></a>`operation_id` | list_lifecycle_events |
| <a id="s-e5f0039374"></a>`path` | /v1/events |
| <a id="s-23fac4683c"></a>`provider_evidence` | None |
| <a id="s-5e10c61541"></a>`read_collection` | {"cursor_parameter": "after", "kind": "cursor-feed", "limit_parameter": "limit"} |
| <a id="s-8c9b6ad6c4"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/events](../http/get-v1-events.md)
- [piggity event list](../../piggity/cli/piggity-event-list.md)

## Governing policies

- <a id="pa-67ac12e521"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-dbd0a65d51"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ec48fc3180"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/92`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 300db08967b5419dbd0cc793dafc6b232dd99eb7ba3190a7c2cf75d4bcf167f1 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "event list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_lifecycle_events",
  "path": "/v1/events",
  "provider_evidence": null,
  "read_collection": {
    "cursor_parameter": "after",
    "kind": "cursor-feed",
    "limit_parameter": "limit"
  },
  "response_authority": "canonical-document"
}
```
