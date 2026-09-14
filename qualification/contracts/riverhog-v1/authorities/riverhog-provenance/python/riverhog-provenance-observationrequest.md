# riverhog_provenance.ObservationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-observationrequest:bce9e95ed0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b87a0d621"></a>
- <a id="s-50e8adf922"></a>`distribution`: `riverhog-provenance`
- <a id="s-0149f17c28"></a>`module`: `riverhog_provenance`
- <a id="s-6581557e3a"></a>`name`: `ObservationRequest`
- <a id="s-a017cd6e75"></a>`unit`: `export`

### Declared structure

- <a id="s-bac5aedba0"></a>`kind`: `"class"`
- <a id="s-5dd12d198a"></a>`signature`: `"\"(path: 'PathInput', lineage_id: 'str', host_id: 'str', host_entity_id: 'str \| None' = None, observer_agent_id: 'str \| None' = None, state_id: 'str \| None' = None, capture_id: 'str \| None' = None, environment_id: 'str \| None' = None, binding_id: 'str \| None' = None, payload_binding: 'PayloadBindingRequest \| None' = None, policy: 'ObservationPolicy' = <factory>, additional_agents: 'tuple[Mapping[str, Any], ...]' = (), additional_associations: 'tuple[Mapping[str, Any], ...]' = (), notes: 'tuple[str, ...]' = ()) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-12aa94106c"></a>`path` | `'PathInput'` | `required` |
| <a id="s-e7749b6e45"></a>`lineage_id` | `'str'` | `required` |
| <a id="s-4cff844118"></a>`host_id` | `'str'` | `required` |
| <a id="s-20d77f421e"></a>`host_entity_id` | `'str \| None'` | `None` |
| <a id="s-51b9439345"></a>`observer_agent_id` | `'str \| None'` | `None` |
| <a id="s-99ebddef59"></a>`state_id` | `'str \| None'` | `None` |
| <a id="s-9d5d40e88f"></a>`capture_id` | `'str \| None'` | `None` |
| <a id="s-9d90afad39"></a>`environment_id` | `'str \| None'` | `None` |
| <a id="s-ad1ae50628"></a>`binding_id` | `'str \| None'` | `None` |
| <a id="s-6b69a407ca"></a>`payload_binding` | `'PayloadBindingRequest \| None'` | `None` |
| <a id="s-9af8101fb0"></a>`policy` | `'ObservationPolicy'` | `factory` |
| <a id="s-39d1722d45"></a>`additional_agents` | `'tuple[Mapping[str, Any], ...]'` | `()` |
| <a id="s-3cf091a32a"></a>`additional_associations` | `'tuple[Mapping[str, Any], ...]'` | `()` |
| <a id="s-a0a718cf51"></a>`notes` | `'tuple[str, ...]'` | `()` |

## Governing policies

- <a id="pa-3c61537f75"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ObservationRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 227023f68d3460a78f3d5cddb2a3183bba1a8bcbc4d51bd02989fb4e3ca0ac4a -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "path",
        "type": "'PathInput'"
      },
      {
        "default": "required",
        "name": "lineage_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "host_id",
        "type": "'str'"
      },
      {
        "default": "None",
        "name": "host_entity_id",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "observer_agent_id",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "state_id",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "capture_id",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "environment_id",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "binding_id",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "payload_binding",
        "type": "'PayloadBindingRequest | None'"
      },
      {
        "default": "factory",
        "name": "policy",
        "type": "'ObservationPolicy'"
      },
      {
        "default": "()",
        "name": "additional_agents",
        "type": "'tuple[Mapping[str, Any], ...]'"
      },
      {
        "default": "()",
        "name": "additional_associations",
        "type": "'tuple[Mapping[str, Any], ...]'"
      },
      {
        "default": "()",
        "name": "notes",
        "type": "'tuple[str, ...]'"
      }
    ],
    "kind": "class",
    "signature": "\"(path: 'PathInput', lineage_id: 'str', host_id: 'str', host_entity_id: 'str | None' = None, observer_agent_id: 'str | None' = None, state_id: 'str | None' = None, capture_id: 'str | None' = None, environment_id: 'str | None' = None, binding_id: 'str | None' = None, payload_binding: 'PayloadBindingRequest | None' = None, policy: 'ObservationPolicy' = <factory>, additional_agents: 'tuple[Mapping[str, Any], ...]' = (), additional_associations: 'tuple[Mapping[str, Any], ...]' = (), notes: 'tuple[str, ...]' = ()) -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ObservationRequest",
  "unit": "export"
}
```
