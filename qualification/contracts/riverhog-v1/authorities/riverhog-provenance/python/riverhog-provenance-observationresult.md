# riverhog_provenance.ObservationResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-observationresult:0d1253c9ca -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8c3043c1c7"></a>
- <a id="s-4805089464"></a>`distribution`: `riverhog-provenance`
- <a id="s-55bc26b6fa"></a>`module`: `riverhog_provenance`
- <a id="s-22021258db"></a>`name`: `ObservationResult`
- <a id="s-a7722ae7bb"></a>`unit`: `export`

### Declared structure

- <a id="s-50ce0b89cd"></a>`kind`: `"class"`
- <a id="s-6c13228d26"></a>`signature`: `"\"(state: 'JsonObject', capture: 'JsonObject', environment: 'JsonObject', agents: 'tuple[JsonObject, ...]', payload_bindings: 'tuple[JsonObject, ...]' = (), extensions: 'tuple[JsonObject, ...]' = ()) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-3274cf9f58"></a>`state` | `'JsonObject'` | `required` |
| <a id="s-1d7ca4c29a"></a>`capture` | `'JsonObject'` | `required` |
| <a id="s-37d0a60426"></a>`environment` | `'JsonObject'` | `required` |
| <a id="s-5d91605620"></a>`agents` | `'tuple[JsonObject, ...]'` | `required` |
| <a id="s-c0522dc334"></a>`payload_bindings` | `'tuple[JsonObject, ...]'` | `()` |
| <a id="s-534e193dac"></a>`extensions` | `'tuple[JsonObject, ...]'` | `()` |

## Maintained corroboration

### Related interface records

- [riverhog_provenance.ObservationResult.assertion_body](riverhog-provenance-observationresult-assertion-body.md)
- [riverhog_provenance.ObservationResult.graph_fragment](riverhog-provenance-observationresult-graph-fragment.md)
- [riverhog_provenance.ObservationResult.make_assertion_entry](riverhog-provenance-observationresult-make-assertion-entry.md)
- [riverhog_provenance.ObservationResult.payload_binding](riverhog-provenance-observationresult-payload-binding.md)

## Governing policies

- <a id="pa-3f30d0b1c1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ObservationResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a279269697444b7c4fb2bfe8fe3f6623c4b009830be0500ca9879d83ab4f49f6 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "state",
        "type": "'JsonObject'"
      },
      {
        "default": "required",
        "name": "capture",
        "type": "'JsonObject'"
      },
      {
        "default": "required",
        "name": "environment",
        "type": "'JsonObject'"
      },
      {
        "default": "required",
        "name": "agents",
        "type": "'tuple[JsonObject, ...]'"
      },
      {
        "default": "()",
        "name": "payload_bindings",
        "type": "'tuple[JsonObject, ...]'"
      },
      {
        "default": "()",
        "name": "extensions",
        "type": "'tuple[JsonObject, ...]'"
      }
    ],
    "kind": "class",
    "signature": "\"(state: 'JsonObject', capture: 'JsonObject', environment: 'JsonObject', agents: 'tuple[JsonObject, ...]', payload_bindings: 'tuple[JsonObject, ...]' = (), extensions: 'tuple[JsonObject, ...]' = ()) -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ObservationResult",
  "unit": "export"
}
```
