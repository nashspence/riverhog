# gogurt_macos_listener_host.listener_adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host-listener-adapter:5555a2af70 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8d25833f10"></a>
- <a id="s-01eb1e8911"></a>`distribution`: `gogurt-macos-listener-host`
- <a id="s-45ad6443ec"></a>`module`: `gogurt_macos_listener_host`
- <a id="s-b9324beb2d"></a>`name`: `listener_adapter`
- <a id="s-596c6d7d47"></a>`unit`: `export`

### Declared structure

- <a id="s-fb4426ba38"></a>`kind`: `"function"`
- <a id="s-3ec4ad5d83"></a>`signature`: `"\"(*, environment: 'Mapping[str, str] \| None' = None, home: 'Path \| None' = None) -> 'ListenerAdapter'\""`

## Governing policies

- <a id="pa-ffb2647e21"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources.md#src-3a09f7fc10) — `reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_macos_listener_host.listener_adapter`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eaa29d11398cb3afaa5279f651114081b2803d5240dabcd4a120a43d2e033582 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerAdapter'\""
  },
  "distribution": "gogurt-macos-listener-host",
  "module": "gogurt_macos_listener_host",
  "name": "listener_adapter",
  "unit": "export"
}
```
