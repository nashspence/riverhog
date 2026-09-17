# gogurt_core.MountedMarkerObservation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-mountedmarkerobservation:e0be34ef87 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-001f059d6c"></a>
- <a id="s-5b9f2b57e3"></a>`distribution`: `gogurt-core`
- <a id="s-123c5f0288"></a>`module`: `gogurt_core`
- <a id="s-c173a77694"></a>`name`: `MountedMarkerObservation`
- <a id="s-c074d35bfd"></a>`unit`: `export`

### Declared structure

- <a id="s-6e6cb9d4e8"></a>`kind`: `"class"`
- <a id="s-35a30b6d46"></a>`signature`: `"\"(marker: 'GogurtRouteMarker', identity: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-c6036c7275"></a>`marker` | `'GogurtRouteMarker'` | `required` |
| <a id="s-050b3f7397"></a>`identity` | `'str'` | `required` |

## Governing policies

- <a id="pa-ea4c58ee7b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.MountedMarkerObservation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4cd8ecafcdfbd1e5f5b399d17341721c3e85e1e782ff3b092f391a327e3190db -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "marker",
        "type": "'GogurtRouteMarker'"
      },
      {
        "default": "required",
        "name": "identity",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(marker: 'GogurtRouteMarker', identity: 'str') -> None\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "MountedMarkerObservation",
  "unit": "export"
}
```

</details>
