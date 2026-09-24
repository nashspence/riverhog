# gogurt_listener_runtime.LISTENER_HEARTBEAT_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listener-heartbeat-format:d4d80ffc3f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b56b19afd2"></a>
- <a id="s-b2d295023c"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-83ac368c80"></a>`module`: `gogurt_listener_runtime`
- <a id="s-159aa415c2"></a>`name`: `LISTENER_HEARTBEAT_FORMAT`
- <a id="s-eb4ac028b7"></a>`unit`: `export`

### Declared structure

- <a id="s-9e525f0c6f"></a>`kind`: `"constant"`
- <a id="s-f6c92dc48a"></a>`value`: `"gogurt-listener-heartbeat/v1"`

## Governing policies

- <a id="pa-c4bff37f28"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.LISTENER_HEARTBEAT_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2b64ca148bd38ab63b73635c2b91b0224fb872c2685e2d5bd180f2f4ee8af3f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-listener-heartbeat/v1"
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "LISTENER_HEARTBEAT_FORMAT",
  "unit": "export"
}
```

</details>
