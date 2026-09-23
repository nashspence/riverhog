# a_gogurt_macos_volume.discover_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-volume:a-gogurt-macos-volume-discover-mount-points:258460a47d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-66085c0774"></a>
- <a id="s-512d7f3795"></a>`distribution`: `a-gogurt-macos-volume`
- <a id="s-dd454bb3fd"></a>`module`: `a_gogurt_macos_volume`
- <a id="s-f182cad4b2"></a>`name`: `discover_mount_points`
- <a id="s-470026601a"></a>`unit`: `export`

### Declared structure

- <a id="s-3777d76d85"></a>`kind`: `"function"`
- <a id="s-c162cf3c02"></a>`signature`: `"\"() -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-aa52bc24b0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-volume:a_gogurt_macos_volume](../../../evidence/sources/authorities.md#src-febb07a039) — [some-implementations/gogurt/mounted-volume/macos/src/a\_gogurt\_macos\_volume/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/macos/src/a_gogurt_macos_volume/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_volume.discover_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f9aab873f81a16a2481ad620beaadd28a1a3944134c92d5fea19232cd4662b3 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'tuple[Path, ...]'\""
  },
  "distribution": "a-gogurt-macos-volume",
  "module": "a_gogurt_macos_volume",
  "name": "discover_mount_points",
  "unit": "export"
}
```

</details>
