# Operation parity: list_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-list-events:77134fed8b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [events](families/events/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9da2a83f6a"></a>
| Concern | Contract |
|---|---|
| <a id="s-446256f618"></a>`application` | stove0 |
| <a id="s-9e299d1986"></a>`classification` | human-cli+json |
| <a id="s-d0d72d16f5"></a>`cli_commands` | ["event list"] |
| <a id="s-f062cdc47e"></a>`client` | Stove0ApiClient |
| <a id="s-5c12bb8e4b"></a>`method` | GET |
| <a id="s-1751856374"></a>`operation_id` | list_events |
| <a id="s-49c25c115c"></a>`path` | /v1/events |
| <a id="s-99a785074e"></a>`provider_evidence` | None |
| <a id="s-503b8be79b"></a>`read_collection` | {"cursor_parameter": "after", "kind": "cursor-feed", "limit_parameter": "limit"} |
| <a id="s-c6da41e41f"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/events](../http/get-v1-events.md)
- [stove0 event list](../cli/stove0-event-list.md)

## Governing policies

- <a id="pa-89f328309f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-118d8f17c6"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-7bc5f0abea"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/131`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 228f282bb54b7032346e272d028770a325fda12102aef4d7dae9f41f3b364a08 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "event list"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "list_events",
  "path": "/v1/events",
  "provider_evidence": null,
  "read_collection": {
    "cursor_parameter": "after",
    "kind": "cursor-feed",
    "limit_parameter": "limit"
  },
  "response_authority": "operator-projection"
}
```
