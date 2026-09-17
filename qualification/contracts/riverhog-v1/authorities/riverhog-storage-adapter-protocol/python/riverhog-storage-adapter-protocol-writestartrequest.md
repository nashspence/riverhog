# riverhog_storage_adapter_protocol.WriteStartRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writestartrequest:5e161bc919 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1d2597b3cb"></a>
- <a id="s-6efb6d803a"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-48acf396cf"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-5ee1a04418"></a>`name`: `WriteStartRequest`
- <a id="s-c445dfbb70"></a>`unit`: `export`

### Declared structure

- <a id="s-7dbcaedcab"></a>`kind`: `"class"`
- <a id="s-66089900c1"></a>`signature`: `"\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: Annotated[int, Ge(ge=1)], content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], placement: Literal['archive', 'immediate']) -> None\""`

#### Validated model schema

<a id="s-5718ecb230"></a>

- <a id="s-ca9f1d8fd4"></a>`type`: `"object"`
- <a id="s-85823fc036"></a>`additionalProperties`: `false`
- <a id="s-389d33dd35"></a>`required`: `["object_path","expected_bytes","content_type","required_identity_assertions","placement"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-66562d712e"></a>`content_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-0920fe8051"></a>`expected_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-cb6fed8c86"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-5911af9f98"></a>`placement` | yes | type="string"; enum=["archive","immediate"] |  |
| <a id="s-f32bac3861"></a>`required_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} |  |

## Maintained corroboration

### Related interface records

- [canonical_metadata](riverhog-storage-adapter-protocol-writestartrequest-canonical-metadata.md)
- [canonical_path](riverhog-storage-adapter-protocol-writestartrequest-canonical-path.md)

## Governing policies

- <a id="pa-45ee3a3d90"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteStartRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f3f2a7509c39f11c95f056a4e71780d73a6cf0aec13c9d90dea603997a397ae5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "content_type": {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        "expected_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "placement": {
          "enum": [
            "archive",
            "immediate"
          ],
          "type": "string"
        },
        "required_identity_assertions": {
          "additionalProperties": {
            "type": "string"
          },
          "maxProperties": 64,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 16384,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-object-identity-assertion-envelope"
          }
        }
      },
      "required": [
        "object_path",
        "expected_bytes",
        "content_type",
        "required_identity_assertions",
        "placement"
      ],
      "type": "object"
    },
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: Annotated[int, Ge(ge=1)], content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], placement: Literal['archive', 'immediate']) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteStartRequest",
  "unit": "export"
}
```

</details>
