# gogurt_path_volume_support.MAX_PATH_MARKER_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-path-volume-support:gogurt-path-volume-support-max-path-marker-bytes:7cfbef9252 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-path-volume-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7a050fe6cc"></a>
- <a id="s-86b0a62592"></a>`distribution`: `gogurt-path-volume-support`
- <a id="s-960ed6dcce"></a>`module`: `gogurt_path_volume_support`
- <a id="s-b01866c1bc"></a>`name`: `MAX_PATH_MARKER_BYTES`
- <a id="s-454e724615"></a>`unit`: `export`

### Declared structure

- <a id="s-06be91e4bb"></a>`kind`: `"constant"`
- <a id="s-eea04195e1"></a>`value`: `4096`

## Governing policies

- <a id="pa-fcae99c8cb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-path-volume-support:gogurt_path_volume_support](../../../evidence/sources/authorities.md#src-a67d948855) — [reference/gogurt/mounted-volume/path-support/src/gogurt\_path\_volume\_support/\_\_init\_\_.py](../../../../../../reference/gogurt/mounted-volume/path-support/src/gogurt_path_volume_support/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_path_volume_support.MAX_PATH_MARKER_BYTES`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c17b9d2248b1a365d22b6924f3df5eb485fb46d803e4f163ebaef7276d8920d6 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 4096
  },
  "distribution": "gogurt-path-volume-support",
  "module": "gogurt_path_volume_support",
  "name": "MAX_PATH_MARKER_BYTES",
  "unit": "export"
}
```

</details>
