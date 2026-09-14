# gogurt_macos_listener_host.LaunchdUserAdapter.start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host-launchduseradapter-start:e002ca27da -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f5360f2b8b"></a>
- <a id="s-a3a7b802e3"></a>`distribution`: `gogurt-macos-listener-host`
- <a id="s-432e803edb"></a>`module`: `gogurt_macos_listener_host`
- <a id="s-252340e715"></a>`name`: `start`
- <a id="s-8a3ec58477"></a>`owner`: `gogurt_macos_listener_host.LaunchdUserAdapter`
- <a id="s-3e69758227"></a>`unit`: `member`

### Declared structure

- <a id="s-86cc481115"></a>`kind`: `"method"`
- <a id="s-94df66491f"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [gogurt_macos_listener_host.LaunchdUserAdapter](gogurt-macos-listener-host-launchduseradapter.md)

## Governing policies

- <a id="pa-327303b96f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources.md#src-3a09f7fc10) — `reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_macos_listener_host.LaunchdUserAdapter.start`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a54b8938dc2cbd6fbe01e2f92850a9d82082a80f2dfb895967516f10577f2cf5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-macos-listener-host",
  "module": "gogurt_macos_listener_host",
  "name": "start",
  "owner": "gogurt_macos_listener_host.LaunchdUserAdapter",
  "unit": "member"
}
```
