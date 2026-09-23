# riverhog_storage_adapter_protocol.MaintenanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-maintenanceresult:07072a2098 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ed8d421fa7"></a>
- <a id="s-939249e3ca"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-26af95f59e"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-5e932f383e"></a>`name`: `MaintenanceResult`
- <a id="s-f628dee5c7"></a>`unit`: `export`

### Declared structure

- <a id="s-e361dbeb5e"></a>`kind`: `"class"`
- <a id="s-33909c3c6d"></a>`signature`: `"'(*, affected: NonnegativeDecimal) -> None'"`

#### Validated model schema

<a id="s-ac14e0db41"></a>

- <a id="s-8724fd62cc"></a>`type`: `"object"`
- <a id="s-57dffc7a04"></a>`additionalProperties`: `false`
- <a id="s-b1c5cfdccd"></a>`required`: `["affected"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-17e08d246a"></a>`affected` | yes | [NonnegativeDecimal](#s-62760472b6) |  |

##### Definitions

- [NonnegativeDecimal](#s-62760472b6)

##### <a id="s-62760472b6"></a>definition `NonnegativeDecimal`

- <a id="s-ee81e4fefc"></a>`type`: `"string"`
- <a id="s-a1584e6171"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Governing policies

- <a id="pa-fad0f8f5cc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.MaintenanceResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de500180b4e31b3325eb23f6ef74a6b9d71fd1f07c086f5613eea83ec8b3d5e3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "affected": {
          "$ref": "#/$defs/NonnegativeDecimal"
        }
      },
      "required": [
        "affected"
      ],
      "type": "object"
    },
    "signature": "'(*, affected: NonnegativeDecimal) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "MaintenanceResult",
  "unit": "export"
}
```

</details>
