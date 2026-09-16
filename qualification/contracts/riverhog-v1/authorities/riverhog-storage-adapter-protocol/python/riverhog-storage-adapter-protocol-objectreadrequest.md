# riverhog_storage_adapter_protocol.ObjectReadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectreadrequest:e055e24268 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-23b3da4fbd"></a>
- <a id="s-4a3e76f2a5"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-056463f18c"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-e5ccef96ef"></a>`name`: `ObjectReadRequest`
- <a id="s-97a6598fda"></a>`unit`: `export`

### Declared structure

- <a id="s-a9a947ce74"></a>`kind`: `"class"`
- <a id="s-885b56accb"></a>`signature`: `"'(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, expected_bytes: Annotated[int, Ge(ge=0)], offset: Annotated[int \| None, Ge(ge=0)] = None, size: Annotated[int \| None, Ge(ge=0)] = None) -> None'"`

#### Validated model schema

<a id="s-6c33cc42f3"></a>

- <a id="s-3aa6510c5a"></a>`type`: `"object"`
- <a id="s-866e7825a7"></a>`additionalProperties`: `false`
- <a id="s-60d5a92908"></a>`required`: `["object","expected_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7d79767301"></a>`expected_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-bee60b01d3"></a>`object` | yes | [ObjectLocator](#s-a7b8fb18e2) |  |
| <a id="s-59bb88205a"></a>`offset` | no | anyOf=(type="integer"; minimum=0) \| (type="null"); default=null |  |
| <a id="s-a6488f469a"></a>`size` | no | anyOf=(type="integer"; minimum=0) \| (type="null"); default=null |  |

##### Definitions

- [ObjectLocator](#s-a7b8fb18e2)

##### <a id="s-a7b8fb18e2"></a>definition `ObjectLocator`

- <a id="s-07819f5058"></a>`type`: `"object"`
- <a id="s-12f4822b76"></a>`additionalProperties`: `false`
- <a id="s-b2f050e0e8"></a>`required`: `["object_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cd6d128d0c"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-747eb445af"></a>`revision` | no | anyOf=(type="string"; maxLength=2000; minLength=1) \| (type="null"); default=null |  |

## Maintained corroboration

### Related interface records

- [validate_range](riverhog-storage-adapter-protocol-objectreadrequest-validate-range.md)

## Governing policies

- <a id="pa-ebd7c4eec6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectReadRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a5a454d62e4183bea7cbc2a9c6eee49c9368a7abdc25a962f0fa84e857ce9e2 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ObjectLocator": {
          "additionalProperties": false,
          "properties": {
            "object_path": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "revision": {
              "anyOf": [
                {
                  "maxLength": 2000,
                  "minLength": 1,
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
            "object_path"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "expected_bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "object": {
          "$ref": "#/$defs/ObjectLocator"
        },
        "offset": {
          "anyOf": [
            {
              "minimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "size": {
          "anyOf": [
            {
              "minimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "object",
        "expected_bytes"
      ],
      "type": "object"
    },
    "signature": "'(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, expected_bytes: Annotated[int, Ge(ge=0)], offset: Annotated[int | None, Ge(ge=0)] = None, size: Annotated[int | None, Ge(ge=0)] = None) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectReadRequest",
  "unit": "export"
}
```

</details>
