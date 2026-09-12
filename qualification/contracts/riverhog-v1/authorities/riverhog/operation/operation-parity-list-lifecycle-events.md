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

<a id="s-da9981e7c010"></a>
| Concern | Contract |
|---|---|
| <a id="s-b456656ecb9d"></a>`application` | riverhog |
| <a id="s-84ebd7f1d18e"></a>`classification` | human-cli+json |
| <a id="s-cb54a42afcf1"></a>`cli_commands` | ["event list"] |
| <a id="s-e8d51acd367d"></a>`client` | ApiClient |
| <a id="s-af74df64966e"></a>`method` | GET |
| <a id="s-654befd32615"></a>`operation_id` | list_lifecycle_events |
| <a id="s-e5f003937444"></a>`path` | /v1/events |
| <a id="s-23fac4683c1a"></a>`provider_evidence` | None |
| <a id="s-5e10c6154142"></a>`read_collection` | {"cursor_parameter": "after", "kind": "cursor-feed", "limit_parameter": "limit"} |
| <a id="s-8c9b6ad6c47c"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/events](../http/get-v1-events.md)
- [piggity event list](../../piggity/cli/piggity-event-list.md)

## Governing policies

- <a id="pa-67ac12e52120"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-dbd0a65d5191"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-ec48fc318000"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
