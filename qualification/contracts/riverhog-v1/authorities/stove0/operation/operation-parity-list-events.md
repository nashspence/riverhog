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

<a id="s-9da2a83f6a95"></a>
| Concern | Contract |
|---|---|
| <a id="s-446256f618a5"></a>`application` | stove0 |
| <a id="s-9e299d198630"></a>`classification` | human-cli+json |
| <a id="s-d0d72d16f53f"></a>`cli_commands` | ["event list"] |
| <a id="s-f062cdc47ea9"></a>`client` | Stove0ApiClient |
| <a id="s-5c12bb8e4b3d"></a>`method` | GET |
| <a id="s-1751856374ce"></a>`operation_id` | list_events |
| <a id="s-49c25c115caa"></a>`path` | /v1/events |
| <a id="s-99a785074ed5"></a>`provider_evidence` | None |
| <a id="s-503b8be79b4c"></a>`read_collection` | {"cursor_parameter": "after", "kind": "cursor-feed", "limit_parameter": "limit"} |
| <a id="s-c6da41e41fba"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/events](../http/get-v1-events.md)
- [stove0 event list](../cli/stove0-event-list.md)

## Governing policies

- <a id="pa-89f328309fd4"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-118d8f17c62f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-7bc5f0abea5a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
