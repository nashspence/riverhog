# riverhog_storage_adapter_protocol.WriteSegmentReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writese-8da51dfdd8:6ce8dbe968 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4981821f64"></a>
- <a id="s-23c4d39d68"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-68537c251b"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-40e101eb69"></a>`name`: `WriteSegmentReceipt`
- <a id="s-3b42ed9c50"></a>`unit`: `export`

### Declared structure

- <a id="s-3443b7a8ad"></a>`kind`: `"class"`
- <a id="s-e987d5edeb"></a>`signature`: `"\"(*, number: Annotated[int, Ge(ge=1)], segment_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)], stored_bytes: Annotated[int, Ge(ge=1)], stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None\""`

#### Validated model schema

<a id="s-50b14a6c22"></a>

- <a id="s-875bd83b21"></a>`type`: `"object"`
- <a id="s-248c995131"></a>`additionalProperties`: `false`
- <a id="s-f0a6b89a1d"></a>`required`: `["number","segment_token","stored_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-41111b1770"></a>`number` | yes | type="integer"; minimum=1 |  |
| <a id="s-8e844b4616"></a>`segment_token` | yes | type="string"; maxLength=4000; minLength=1 |  |
| <a id="s-2114107bd7"></a>`stored_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-a22f06e6d5"></a>`stored_sha256` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |

## Governing policies

- <a id="pa-47471a17ec"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSegmentReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1d481fd51eb44770f0deb3d5c419abf4f89f4db6e0d8749705ec34cb748254d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "number": {
          "minimum": 1,
          "type": "integer"
        },
        "segment_token": {
          "maxLength": 4000,
          "minLength": 1,
          "type": "string"
        },
        "stored_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "stored_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "number",
        "segment_token",
        "stored_bytes"
      ],
      "type": "object"
    },
    "signature": "\"(*, number: Annotated[int, Ge(ge=1)], segment_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)], stored_bytes: Annotated[int, Ge(ge=1)], stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteSegmentReceipt",
  "unit": "export"
}
```

</details>
