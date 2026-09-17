# riverhog_protocol.CapabilityAction

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-capabilityaction:91c7fb60dd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a73cdfacfa"></a>
- <a id="s-b989b6413a"></a>`distribution`: `riverhog-protocol`
- <a id="s-1f9d146ceb"></a>`module`: `riverhog_protocol`
- <a id="s-a223e0d630"></a>`name`: `CapabilityAction`
- <a id="s-42d9787583"></a>`unit`: `export`

### Declared structure

- <a id="s-a34c407b81"></a>`kind`: `"object"`
- <a id="s-9d17aa40c1"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-e9d7ecf39b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CapabilityAction`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 67f7544db3334b1eed3922c143bfc05ad8a0330d0834dc331f142a703e4a4d94 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CapabilityAction",
  "unit": "export"
}
```

</details>
