# riverhog_protocol.COLLECTION_WAKE_EVENT_TYPES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-wake-event-types:528a441e7b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4ed5eef024"></a>
- <a id="s-f614e103af"></a>`distribution`: `riverhog-protocol`
- <a id="s-5739addd28"></a>`module`: `riverhog_protocol`
- <a id="s-293b1b4991"></a>`name`: `COLLECTION_WAKE_EVENT_TYPES`
- <a id="s-51e78607ec"></a>`unit`: `export`

### Declared structure

- <a id="s-e74d52a5a9"></a>`kind`: `"constant"`
- <a id="s-9459dad103"></a>`value`: `["io.riverhog.riverhog.collection.finalized"]`

## Governing policies

- <a id="pa-063d92b2a7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.COLLECTION_WAKE_EVENT_TYPES`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4042c4ebeed6d63e47b0d2314da13010014551fc72cd7e7e77fc350c8031fdfd -->

```json
{
  "contract": {
    "kind": "constant",
    "value": [
      "io.riverhog.riverhog.collection.finalized"
    ]
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "COLLECTION_WAKE_EVENT_TYPES",
  "unit": "export"
}
```

</details>
