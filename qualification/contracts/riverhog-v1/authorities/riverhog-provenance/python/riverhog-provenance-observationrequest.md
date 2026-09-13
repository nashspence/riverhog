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
| Field | Shape |
|---|---|
| <a id="s-645108068d"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-50e8adf922"></a>`distribution` | "riverhog-provenance" |
| <a id="s-0149f17c28"></a>`module` | "riverhog_provenance" |
| <a id="s-6581557e3a"></a>`name` | "ObservationRequest" |
| <a id="s-a017cd6e75"></a>`unit` | "export" |

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
