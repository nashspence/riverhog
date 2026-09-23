# riverhog_storage_adapter_protocol.WriteSession

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writesession:091526f7c8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4739bfe396"></a>
- <a id="s-cd3f005fb9"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-aff1663f8b"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-a739a910a0"></a>`name`: `WriteSession`
- <a id="s-787c3950e7"></a>`unit`: `export`

### Declared structure

- <a id="s-d667f20b9e"></a>`kind`: `"class"`
- <a id="s-4017fac2a6"></a>`signature`: `"'(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: PositiveDecimal, write_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None'"`

#### Validated model schema

<a id="s-e98562805e"></a>

- <a id="s-aeb525a1a5"></a>`type`: `"object"`
- <a id="s-4783edf0b8"></a>`additionalProperties`: `false`
- <a id="s-9d99763b25"></a>`required`: `["object_path","expected_bytes","write_token"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a3bee6e0ec"></a>`expected_bytes` | yes | [PositiveDecimal](#s-9957c8fcc7) |  |
| <a id="s-7927a255de"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-7803618695"></a>`write_token` | yes | type="string"; maxLength=4000; minLength=1 |  |

##### Definitions

- [PositiveDecimal](#s-9957c8fcc7)

##### <a id="s-9957c8fcc7"></a>definition `PositiveDecimal`

- <a id="s-8736df35ee"></a>`type`: `"string"`
- <a id="s-75dc73b735"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [canonical_path](riverhog-storage-adapter-protocol-writesession-canonical-path.md)

## Governing policies

- <a id="pa-f6dba90dbe"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSession`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: efd49ba72c66c95e9065249ac98495e8d25ade82c44b4b9601325f7be7802a43 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "PositiveDecimal": {
          "pattern": "^[1-9][0-9]*(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "expected_bytes": {
          "$ref": "#/$defs/PositiveDecimal"
        },
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "write_token": {
          "maxLength": 4000,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "object_path",
        "expected_bytes",
        "write_token"
      ],
      "type": "object"
    },
    "signature": "'(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: PositiveDecimal, write_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteSession",
  "unit": "export"
}
```

</details>
