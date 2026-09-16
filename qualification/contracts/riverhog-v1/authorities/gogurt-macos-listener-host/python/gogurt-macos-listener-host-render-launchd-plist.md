# gogurt_macos_listener_host.render_launchd_plist

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host-render-launchd-plist:a9421a7bda -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fa52bc6edc"></a>
- <a id="s-697cbdae8b"></a>`distribution`: `gogurt-macos-listener-host`
- <a id="s-e5344fdd3e"></a>`module`: `gogurt_macos_listener_host`
- <a id="s-72556221ad"></a>`name`: `render_launchd_plist`
- <a id="s-d885dd0b46"></a>`unit`: `export`

### Declared structure

- <a id="s-30b0abc5dd"></a>`kind`: `"function"`
- <a id="s-b297878157"></a>`signature`: `"\"(command: 'Sequence[str]') -> 'bytes'\""`

## Governing policies

- <a id="pa-bb247fffdf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources.md#src-3a09f7fc10) — `reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_macos_listener_host.render_launchd_plist`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8bce8423c1dfb40187e2cf8d02b4c0e57ac442173734c110f0de21d072105523 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(command: 'Sequence[str]') -> 'bytes'\""
  },
  "distribution": "gogurt-macos-listener-host",
  "module": "gogurt_macos_listener_host",
  "name": "render_launchd_plist",
  "unit": "export"
}
```

</details>
