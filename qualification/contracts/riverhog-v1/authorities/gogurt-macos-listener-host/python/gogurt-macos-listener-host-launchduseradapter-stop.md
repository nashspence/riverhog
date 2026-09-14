# gogurt_macos_listener_host.LaunchdUserAdapter.stop

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host-launchduseradapter-stop:694383a011 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6e3b1c2acf"></a>
- <a id="s-e6fc33fa17"></a>`distribution`: `gogurt-macos-listener-host`
- <a id="s-de293d7f8c"></a>`module`: `gogurt_macos_listener_host`
- <a id="s-3b7009fc80"></a>`name`: `stop`
- <a id="s-82aa58e3ba"></a>`owner`: `gogurt_macos_listener_host.LaunchdUserAdapter`
- <a id="s-b1bce66527"></a>`unit`: `member`

### Declared structure

- <a id="s-45b4557bfc"></a>`kind`: `"method"`
- <a id="s-894840f913"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [gogurt_macos_listener_host.LaunchdUserAdapter](gogurt-macos-listener-host-launchduseradapter.md)

## Governing policies

- <a id="pa-b62419a7cf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources.md#src-3a09f7fc10) — `reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_macos_listener_host.LaunchdUserAdapter.stop`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af8aa43df6f38ea33eb76ca3f2c2e27824912f9d384a4c358ae8b95acb1ee394 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-macos-listener-host",
  "module": "gogurt_macos_listener_host",
  "name": "stop",
  "owner": "gogurt_macos_listener_host.LaunchdUserAdapter",
  "unit": "member"
}
```
