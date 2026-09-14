# riverhog_protocol.RIVERHOG_EVENT_TYPES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-riverhog-event-types:cbc5b89f5b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-92f5c1d452"></a>
- <a id="s-ad731c37a8"></a>`distribution`: `riverhog-protocol`
- <a id="s-8763c4f348"></a>`module`: `riverhog_protocol`
- <a id="s-5efa389865"></a>`name`: `RIVERHOG_EVENT_TYPES`
- <a id="s-8da2196cba"></a>`unit`: `export`

### Declared structure

- <a id="s-72dd4ffd30"></a>`kind`: `"constant"`
- <a id="s-ab76d180e4"></a>`value`: `["io.riverhog.riverhog.archive_copy.canceled","io.riverhog.riverhog.archive_copy.completed","io.riverhog.riverhog.archive_copy.issue","io.riverhog.riverhog.archive_copy.requested","io.riverhog.riverhog.collection.deleted","io.riverhog.riverhog.collection.finalized","io.riverhog.riverhog.retrieval.canceled","io.riverhog.riverhog.retrieval.completed","io.riverhog.riverhog.retrieval.expired","io.riverhog.riverhog.retrieval.failed","io.riverhog.riverhog.retrieval.issue","io.riverhog.riverhog.retrieval.ready","io.riverhog.riverhog.retrieval.renewed","io.riverhog.riverhog.retrieval.requested"]`

## Governing policies

- <a id="pa-07814d1caa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RIVERHOG_EVENT_TYPES`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32832ab5a43c60190dcb60d901ddde8db731112dca9ae5d218b2de6510f05f21 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": [
      "io.riverhog.riverhog.archive_copy.canceled",
      "io.riverhog.riverhog.archive_copy.completed",
      "io.riverhog.riverhog.archive_copy.issue",
      "io.riverhog.riverhog.archive_copy.requested",
      "io.riverhog.riverhog.collection.deleted",
      "io.riverhog.riverhog.collection.finalized",
      "io.riverhog.riverhog.retrieval.canceled",
      "io.riverhog.riverhog.retrieval.completed",
      "io.riverhog.riverhog.retrieval.expired",
      "io.riverhog.riverhog.retrieval.failed",
      "io.riverhog.riverhog.retrieval.issue",
      "io.riverhog.riverhog.retrieval.ready",
      "io.riverhog.riverhog.retrieval.renewed",
      "io.riverhog.riverhog.retrieval.requested"
    ]
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RIVERHOG_EVENT_TYPES",
  "unit": "export"
}
```
