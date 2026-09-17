# gogurt_core.GogurtRouteMarker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurtroutemarker:03e2385c4d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-062f7ad259"></a>
- <a id="s-6c14fd0f43"></a>`distribution`: `gogurt-core`
- <a id="s-5b1d17b159"></a>`module`: `gogurt_core`
- <a id="s-3b24993859"></a>`name`: `GogurtRouteMarker`
- <a id="s-f33be52755"></a>`unit`: `export`

### Declared structure

- <a id="s-23d55dcc30"></a>`kind`: `"class"`
- <a id="s-96d5cfded3"></a>`signature`: `"\"(route: 'str', format: 'str' = 'gogurt-route-marker/v1') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-e599b40571"></a>`route` | `'str'` | `required` |
| <a id="s-d5d44ff7fd"></a>`format` | `'str'` | `'gogurt-route-marker/v1'` |

## Maintained corroboration

### Related interface records

- [as_dict](gogurt-core-gogurtroutemarker-as-dict.md)
- [from_mapping](gogurt-core-gogurtroutemarker-from-mapping.md)

## Governing policies

- <a id="pa-ad62a9f216"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.GogurtRouteMarker`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b617ecefa074b84b3d34ccbc4d88dc35427cffa8e55c3bb49b76621825f1e7b7 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "route",
        "type": "'str'"
      },
      {
        "default": "'gogurt-route-marker/v1'",
        "name": "format",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(route: 'str', format: 'str' = 'gogurt-route-marker/v1') -> None\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GogurtRouteMarker",
  "unit": "export"
}
```

</details>
