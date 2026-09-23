# riverhog_provenance.FileStateObservationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-filestateobservationrequest:049d4862f6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8487addef7"></a>
- <a id="s-550e69fe84"></a>`distribution`: `riverhog-provenance`
- <a id="s-7b59d85528"></a>`module`: `riverhog_provenance`
- <a id="s-a6421d7505"></a>`name`: `FileStateObservationRequest`
- <a id="s-6b46c10888"></a>`unit`: `export`

### Declared structure

- <a id="s-655142225a"></a>`kind`: `"class"`
- <a id="s-b0f152d937"></a>`signature`: `"\"(path: 'PathInput', lineage_id: 'str', host_id: 'str', host_entity_id: 'str \| None' = None, observer_agent_id: 'str \| None' = None, state_id: 'str \| None' = None, capture_id: 'str \| None' = None, environment_id: 'str \| None' = None, binding_id: 'str \| None' = None, payload_binding: 'PayloadBindingRequest \| None' = None, policy: 'ObservationPolicy' = <factory>, additional_agents: 'tuple[Mapping[str, Any], ...]' = (), additional_associations: 'tuple[Mapping[str, Any], ...]' = (), notes: 'tuple[str, ...]' = ()) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-263e36278d"></a>`path` | `'PathInput'` | `required` |
| <a id="s-4e9ee2158b"></a>`lineage_id` | `'str'` | `required` |
| <a id="s-1fa03a7ca6"></a>`host_id` | `'str'` | `required` |
| <a id="s-45d3f54af3"></a>`host_entity_id` | `'str \| None'` | `None` |
| <a id="s-601a4ea57b"></a>`observer_agent_id` | `'str \| None'` | `None` |
| <a id="s-3ee3ff3bdd"></a>`state_id` | `'str \| None'` | `None` |
| <a id="s-4750332801"></a>`capture_id` | `'str \| None'` | `None` |
| <a id="s-44d8c26e25"></a>`environment_id` | `'str \| None'` | `None` |
| <a id="s-e7efbf98ac"></a>`binding_id` | `'str \| None'` | `None` |
| <a id="s-a528c84dbb"></a>`payload_binding` | `'PayloadBindingRequest \| None'` | `None` |
| <a id="s-19284b636c"></a>`policy` | `'ObservationPolicy'` | `factory` |
| <a id="s-2b2a540d74"></a>`additional_agents` | `'tuple[Mapping[str, Any], ...]'` | `()` |
| <a id="s-613110a991"></a>`additional_associations` | `'tuple[Mapping[str, Any], ...]'` | `()` |
| <a id="s-dc779f5e2b"></a>`notes` | `'tuple[str, ...]'` | `()` |

## Governing policies

- <a id="pa-734198d7b4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.FileStateObservationRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95eb4c0200db5d94aaac907f31fd8caf5b10c1834b74b589466159379a1fb264 -->

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
  "name": "FileStateObservationRequest",
  "unit": "export"
}
```

</details>
