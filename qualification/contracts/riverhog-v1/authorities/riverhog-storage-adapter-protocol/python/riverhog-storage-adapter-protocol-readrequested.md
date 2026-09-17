# riverhog_storage_adapter_protocol.ReadRequested

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readrequested:af180e827a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9a7acfd5f1"></a>
- <a id="s-76a87a25ec"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-25cda13643"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-94065ea87d"></a>`name`: `ReadRequested`
- <a id="s-97decf1a13"></a>`unit`: `export`

### Declared structure

- <a id="s-a8427453a0"></a>`kind`: `"class"`
- <a id="s-66ccaec0a7"></a>`signature`: `"\"(*, state: Literal['requested'] = 'requested', estimated_ready_at: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=100)] = None) -> None\""`

#### Validated model schema

<a id="s-5e92215fe7"></a>

- <a id="s-2974f2d622"></a>`type`: `"object"`
- <a id="s-94f373ff48"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2df294917"></a>`estimated_ready_at` | no | anyOf=[(type="string"; maxLength=100; minLength=1); (type="null")]; default=null |  |
| <a id="s-f0a46163f4"></a>`state` | no | type="string"; const="requested"; default="requested" |  |

## Maintained corroboration

### Related interface records

- [canonical_estimated_ready_at](riverhog-storage-adapter-protocol-readrequested-canonical-estimated-ready-at.md)

## Governing policies

- <a id="pa-9df9317e04"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadRequested`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1da6894c05c85076dc3ad7927863e2959374f2bc975543a0c52e45d8ba89b190 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "estimated_ready_at": {
          "anyOf": [
            {
              "maxLength": 100,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "state": {
          "const": "requested",
          "default": "requested",
          "type": "string"
        }
      },
      "type": "object"
    },
    "signature": "\"(*, state: Literal['requested'] = 'requested', estimated_ready_at: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=100)] = None) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ReadRequested",
  "unit": "export"
}
```

</details>
