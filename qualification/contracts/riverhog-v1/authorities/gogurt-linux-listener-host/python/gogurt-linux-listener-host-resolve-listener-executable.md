# gogurt_linux_listener_host.resolve_listener_executable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-resolve-listen-42c1223f30:1349cd4889 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fbaafc3624"></a>
- <a id="s-782ec12b28"></a>`distribution`: `gogurt-linux-listener-host`
- <a id="s-43a37362c8"></a>`module`: `gogurt_linux_listener_host`
- <a id="s-678c531857"></a>`name`: `resolve_listener_executable`
- <a id="s-8098b69214"></a>`unit`: `export`

### Declared structure

- <a id="s-cc07a900e1"></a>`kind`: `"function"`
- <a id="s-439f6ed8a6"></a>`signature`: `"\"(raw: 'str \| None' = None) -> 'Path'\""`

## Governing policies

- <a id="pa-84e07dbb9c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.resolve_listener_executable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a5c421da16abf8d1166717531ea44d151bd64c385c9db3b0693012b328935541 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(raw: 'str | None' = None) -> 'Path'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "resolve_listener_executable",
  "unit": "export"
}
```

</details>
