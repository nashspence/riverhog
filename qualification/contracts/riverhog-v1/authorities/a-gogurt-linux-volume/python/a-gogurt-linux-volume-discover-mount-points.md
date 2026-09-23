# a_gogurt_linux_volume.discover_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-volume:a-gogurt-linux-volume-discover-mount-points:99cc090827 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6788150ff5"></a>
- <a id="s-6ee384ff0e"></a>`distribution`: `a-gogurt-linux-volume`
- <a id="s-f8167662b1"></a>`module`: `a_gogurt_linux_volume`
- <a id="s-af865b1141"></a>`name`: `discover_mount_points`
- <a id="s-f8afd24bc1"></a>`unit`: `export`

### Declared structure

- <a id="s-f1cae1c06d"></a>`kind`: `"function"`
- <a id="s-f659627427"></a>`signature`: `"\"() -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-41d9887db1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-volume:a_gogurt_linux_volume](../../../evidence/sources/authorities.md#src-7cf1a6536b) — [some-implementations/gogurt/mounted-volume/linux/src/a\_gogurt\_linux\_volume/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/linux/src/a_gogurt_linux_volume/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_volume.discover_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa06cd9edb9e3db3d67de6f90c7a681fc0e689d71ecdb725f624c7f471d7315e -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'tuple[Path, ...]'\""
  },
  "distribution": "a-gogurt-linux-volume",
  "module": "a_gogurt_linux_volume",
  "name": "discover_mount_points",
  "unit": "export"
}
```

</details>
