# gogurt_core.iter_new_mounts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-iter-new-mounts:2d4bed3505 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e2dadabf7f"></a>
- <a id="s-036bb6e548"></a>`distribution`: `gogurt-core`
- <a id="s-bbfec0dce9"></a>`module`: `gogurt_core`
- <a id="s-69abb98fac"></a>`name`: `iter_new_mounts`
- <a id="s-2021fadfdb"></a>`unit`: `export`

### Declared structure

- <a id="s-2812443fc3"></a>`kind`: `"function"`
- <a id="s-4a36e5d061"></a>`signature`: `"\"(*, discover: 'Callable[[], Sequence[Path]]', interval_seconds: 'float' = 2.0, include_existing: 'bool' = False, sleep: 'Callable[[float], None]' = <built-in function sleep>) -> 'Iterator[Path]'\""`

## Governing policies

- <a id="pa-e11726c9b2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.iter_new_mounts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5645e2828869d172132730eef0689c77a92b2d8aff876f31d6e7e5e57f535839 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, discover: 'Callable[[], Sequence[Path]]', interval_seconds: 'float' = 2.0, include_existing: 'bool' = False, sleep: 'Callable[[float], None]' = <built-in function sleep>) -> 'Iterator[Path]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "iter_new_mounts",
  "unit": "export"
}
```

</details>
