# riverhog_protocol.RiverhogLifecycleEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-riverhoglifecycleevent:2655ad86bf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-868ddf62ab"></a>
| Field | Shape |
|---|---|
| <a id="s-32d3c45ac5"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-83416f716d"></a>`distribution` | "riverhog-protocol" |
| <a id="s-c32056fd9d"></a>`module` | "riverhog_protocol" |
| <a id="s-9c611d108f"></a>`name` | "RiverhogLifecycleEvent" |
| <a id="s-66ee1010fe"></a>`unit` | "export" |

## Governing policies

- <a id="pa-49e868f71d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RiverhogLifecycleEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6aa3064618823bd9d46594b6de6e465a65e6ddce13693c7133da46b96803421c -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[riverhog_protocol.lifecycle_events.CollectionFinalizedEvent | riverhog_protocol.lifecycle_events.CollectionDeletedEvent | riverhog_protocol.lifecycle_events.ArchiveCopyRequestedEvent | riverhog_protocol.lifecycle_events.ArchiveCopyCompletedEvent | riverhog_protocol.lifecycle_events.ArchiveCopyIssueEvent | riverhog_protocol.lifecycle_events.ArchiveCopyCanceledEvent | riverhog_protocol.lifecycle_events.RetrievalRequestedEvent | riverhog_protocol.lifecycle_events.RetrievalReadyEvent | riverhog_protocol.lifecycle_events.RetrievalRenewedEvent | riverhog_protocol.lifecycle_events.RetrievalCompletedEvent | riverhog_protocol.lifecycle_events.RetrievalCanceledEvent | riverhog_protocol.lifecycle_events.RetrievalExpiredEvent | riverhog_protocol.lifecycle_events.RetrievalIssueEvent | riverhog_protocol.lifecycle_events.RetrievalFailedEvent, FieldInfo(annotation=NoneType, required=True, discriminator='type')]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RiverhogLifecycleEvent",
  "unit": "export"
}
```
