# gogurt_listener_runtime.install_listener

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-install-listener:d4a6c5e3c1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d838ca2a61"></a>
| Field | Shape |
|---|---|
| <a id="s-96909dccb7"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d5ecf51db1"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-9937f6be27"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-0c6dbb5b34"></a>`name` | "install_listener" |
| <a id="s-66ce4d3ec2"></a>`unit` | "export" |

## Governing policies

- <a id="pa-92c956b93b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.install_listener`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b77773ff020880ec18033f70e5e8dbd282f38978673486973172cd8c94402aa7 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(routes_file: 'Path', *, actions_dir: 'Path | None', interval_seconds: 'float' = 2.0, executable: 'Path', paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str', mounted_volume_provider: 'GogurtProviderReference', listener_host_provider: 'GogurtProviderReference', wait_for_health: 'bool' = True) -> 'dict[str, object]'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "install_listener",
  "unit": "export"
}
```
