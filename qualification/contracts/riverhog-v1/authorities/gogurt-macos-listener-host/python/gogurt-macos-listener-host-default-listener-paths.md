# gogurt_macos_listener_host.default_listener_paths

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host-default-listener-paths:6fefce3780 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-abf3ba87d4"></a>
- <a id="s-e7d86a566a"></a>`distribution`: `gogurt-macos-listener-host`
- <a id="s-1ce2c5cb8b"></a>`module`: `gogurt_macos_listener_host`
- <a id="s-a2beca9345"></a>`name`: `default_listener_paths`
- <a id="s-14b46e1ea4"></a>`unit`: `export`

### Declared structure

- <a id="s-74a8e5e442"></a>`kind`: `"function"`
- <a id="s-ecd8254a16"></a>`signature`: `"\"(*, environment: 'Mapping[str, str] \| None' = None, home: 'Path \| None' = None) -> 'ListenerRuntimePaths'\""`

## Governing policies

- <a id="pa-b73f02185d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources.md#src-3a09f7fc10) — `reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_macos_listener_host.default_listener_paths`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f88f0afc1cd2ba43a0e9ab482559fcf3951922d2098d3ed04a5a26c0de6f83b -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerRuntimePaths'\""
  },
  "distribution": "gogurt-macos-listener-host",
  "module": "gogurt_macos_listener_host",
  "name": "default_listener_paths",
  "unit": "export"
}
```

</details>
