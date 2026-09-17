# gogurt_linux_mounted_volume.linux_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-mounted-volume:gogurt-linux-mounted-volume-linux-mount-points:0b70b960dc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-mounted-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9ac2f8ed07"></a>
- <a id="s-5b59b424d7"></a>`distribution`: `gogurt-linux-mounted-volume`
- <a id="s-4775fc8291"></a>`module`: `gogurt_linux_mounted_volume`
- <a id="s-1260a2a4f0"></a>`name`: `linux_mount_points`
- <a id="s-0b1f4622c6"></a>`unit`: `export`

### Declared structure

- <a id="s-cb7bf96e58"></a>`kind`: `"function"`
- <a id="s-8f47e119d3"></a>`signature`: `"\"(mountinfo: 'str') -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-a58f1b893b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-linux-mounted-volume:gogurt_linux_mounted_volume](../../../evidence/sources.md#src-dfbc0b0c2f) — [reference/gogurt/mounted-volume/linux/src/gogurt\_linux\_mounted\_volume/\_\_init\_\_.py](../../../../../../reference/gogurt/mounted-volume/linux/src/gogurt_linux_mounted_volume/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_linux_mounted_volume.linux_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0bf07e46d1544e29fb30d4c721ead4a6672b44bef82be799aed84a5f8a35d643 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(mountinfo: 'str') -> 'tuple[Path, ...]'\""
  },
  "distribution": "gogurt-linux-mounted-volume",
  "module": "gogurt_linux_mounted_volume",
  "name": "linux_mount_points",
  "unit": "export"
}
```

</details>
