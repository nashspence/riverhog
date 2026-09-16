# gogurt_linux_listener_host.listener_adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-listener-adapter:623aa9dffd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d561226c8"></a>
- <a id="s-98a8eba237"></a>`distribution`: `gogurt-linux-listener-host`
- <a id="s-c07e15d780"></a>`module`: `gogurt_linux_listener_host`
- <a id="s-d3764f2c9b"></a>`name`: `listener_adapter`
- <a id="s-b8690c49f9"></a>`unit`: `export`

### Declared structure

- <a id="s-27b5afed40"></a>`kind`: `"function"`
- <a id="s-8441b7be94"></a>`signature`: `"\"(*, environment: 'Mapping[str, str] \| None' = None, home: 'Path \| None' = None) -> 'ListenerAdapter'\""`

## Governing policies

- <a id="pa-b92a4d5eee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.listener_adapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 120b830e7d49b2d7499ea6e51f33ec12b369b316ec78594d2472aff2a656b203 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerAdapter'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "listener_adapter",
  "unit": "export"
}
```

</details>
