# a_gogurt_linux_volume.linux_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-volume:a-gogurt-linux-volume-linux-mount-points:08231dbb39 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-54e9d88402"></a>
- <a id="s-ca1c486a0a"></a>`distribution`: `a-gogurt-linux-volume`
- <a id="s-dba6567089"></a>`module`: `a_gogurt_linux_volume`
- <a id="s-6318341629"></a>`name`: `linux_mount_points`
- <a id="s-16bf6309d8"></a>`unit`: `export`

### Declared structure

- <a id="s-8c66820433"></a>`kind`: `"function"`
- <a id="s-92b68c6fa7"></a>`signature`: `"\"(mountinfo: 'str') -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-60de5cdd04"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-volume:a_gogurt_linux_volume](../../../evidence/sources/authorities.md#src-7cf1a6536b) — [some-implementations/gogurt/mounted-volume/linux/src/a\_gogurt\_linux\_volume/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/linux/src/a_gogurt_linux_volume/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_volume.linux_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a12af9e9ec5656fca9fa32c1b27300be92e402f9b7a81d7eb98fd451b8990e35 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(mountinfo: 'str') -> 'tuple[Path, ...]'\""
  },
  "distribution": "a-gogurt-linux-volume",
  "module": "a_gogurt_linux_volume",
  "name": "linux_mount_points",
  "unit": "export"
}
```

</details>
