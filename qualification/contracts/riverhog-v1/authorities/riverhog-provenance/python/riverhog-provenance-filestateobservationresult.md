# riverhog_provenance.FileStateObservationResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-filestateobservationresult:cd28ccee74 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-19ae6ede90"></a>
- <a id="s-409858a3cd"></a>`distribution`: `riverhog-provenance`
- <a id="s-a2e1365232"></a>`module`: `riverhog_provenance`
- <a id="s-92c688887a"></a>`name`: `FileStateObservationResult`
- <a id="s-e02dfb4bbe"></a>`unit`: `export`

### Declared structure

- <a id="s-8115c0efed"></a>`kind`: `"class"`
- <a id="s-1f657a3711"></a>`signature`: `"\"(file_state: 'JsonObject', capture: 'JsonObject', environment: 'JsonObject', agents: 'tuple[JsonObject, ...]', payload_bindings: 'tuple[JsonObject, ...]' = (), extensions: 'tuple[JsonObject, ...]' = ()) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-0b976095f8"></a>`file_state` | `'JsonObject'` | `required` |
| <a id="s-f29a3d9e7c"></a>`capture` | `'JsonObject'` | `required` |
| <a id="s-542941c449"></a>`environment` | `'JsonObject'` | `required` |
| <a id="s-18c7b0365f"></a>`agents` | `'tuple[JsonObject, ...]'` | `required` |
| <a id="s-43de9ad990"></a>`payload_bindings` | `'tuple[JsonObject, ...]'` | `()` |
| <a id="s-f9938ea02d"></a>`extensions` | `'tuple[JsonObject, ...]'` | `()` |

## Maintained corroboration

### Related interface records

- [assertion_body](riverhog-provenance-filestateobservationresult-assertion-body.md)
- [make_assertion_entry](riverhog-provenance-filestateobservationresult-make-assertion-entry.md)
- [payload_binding](riverhog-provenance-filestateobservationresult-payload-binding.md)
- [graph_fragment](riverhog-provenance-filestateobservationresult-graph-fragment.md)

## Governing policies

- <a id="pa-77e53997a4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.FileStateObservationResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b9c2b290596b16dd155e4df2f6ce33654f2993fb9a7e97251e1b63bcf1e446a -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "file_state",
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
    "signature": "\"(file_state: 'JsonObject', capture: 'JsonObject', environment: 'JsonObject', agents: 'tuple[JsonObject, ...]', payload_bindings: 'tuple[JsonObject, ...]' = (), extensions: 'tuple[JsonObject, ...]' = ()) -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "FileStateObservationResult",
  "unit": "export"
}
```

</details>
