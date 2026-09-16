# gogurt_linux_mounted_volume.discover_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-mounted-volume:gogurt-linux-mounted-volume-discover-mount-points:0345bf955d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-mounted-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75c5bd7d10"></a>
- <a id="s-7bd0ea75bf"></a>`distribution`: `gogurt-linux-mounted-volume`
- <a id="s-6c50eb5a6a"></a>`module`: `gogurt_linux_mounted_volume`
- <a id="s-8a79256821"></a>`name`: `discover_mount_points`
- <a id="s-2438596c07"></a>`unit`: `export`

### Declared structure

- <a id="s-e1ac217220"></a>`kind`: `"function"`
- <a id="s-890d88ee4d"></a>`signature`: `"\"() -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-e1de41388a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-mounted-volume:gogurt_linux_mounted_volume](../../../evidence/sources.md#src-dfbc0b0c2f) — `reference/gogurt/mounted-volume/linux/src/gogurt_linux_mounted_volume/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_linux_mounted_volume.discover_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 54317c0bf2ab90eb69fb6535759a98adb4c1d26a5686559561a3b072abbe00aa -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'tuple[Path, ...]'\""
  },
  "distribution": "gogurt-linux-mounted-volume",
  "module": "gogurt_linux_mounted_volume",
  "name": "discover_mount_points",
  "unit": "export"
}
```

</details>
