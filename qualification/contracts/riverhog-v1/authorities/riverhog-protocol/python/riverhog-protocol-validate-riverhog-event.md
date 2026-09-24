# riverhog_protocol.validate_riverhog_event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-validate-riverhog-event:331cba75cf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-647114ab52"></a>
- <a id="s-af1f0c298f"></a>`distribution`: `riverhog-protocol`
- <a id="s-e2b10e8a90"></a>`module`: `riverhog_protocol`
- <a id="s-a4baa8ed47"></a>`name`: `validate_riverhog_event`
- <a id="s-b82fdbe50d"></a>`unit`: `export`

### Declared structure

- <a id="s-41bd71df81"></a>`kind`: `"function"`
- <a id="s-5c99fe46dc"></a>`signature`: `"\"(value: 'LifecycleEvent \| dict[str, Any]') -> 'RiverhogLifecycleEvent'\""`

## Governing policies

- <a id="pa-fb77422a4f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.validate_riverhog_event`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6b6714d6c3a0123498b3c3254f0c31e57e5c566d8dd753b23ebc11abf7e4211 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'LifecycleEvent | dict[str, Any]') -> 'RiverhogLifecycleEvent'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_riverhog_event",
  "unit": "export"
}
```

</details>
