# riverhog_protocol.ProcessingClaimRenewDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimrenewdocument:55743c6830 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67d8290270"></a>
- <a id="s-409aa45f28"></a>`distribution`: `riverhog-protocol`
- <a id="s-ae7fd162ea"></a>`module`: `riverhog_protocol`
- <a id="s-a6c8ba6b13"></a>`name`: `ProcessingClaimRenewDocument`
- <a id="s-e58af8205b"></a>`unit`: `export`

### Declared structure

- <a id="s-8620053107"></a>`kind`: `"class"`
- <a id="s-c69303d0ee"></a>`signature`: `"'(*, fence: Annotated[NonnegativeDecimal, Ge(ge=1)], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800) -> None'"`

#### Validated model schema

<a id="s-0e65013f90"></a>

- <a id="s-0095a52f9f"></a>`type`: `"object"`
- <a id="s-97774f793c"></a>`additionalProperties`: `false`
- <a id="s-b9adf70659"></a>`required`: `["fence"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c6ad8973f0"></a>`fence` | yes | [NonnegativeDecimal](#s-4e37479e1d); ge=1 |  |
| <a id="s-c75d18c697"></a>`lease_seconds` | no | type="integer"; minimum=30; maximum=86400; default=1800 |  |

##### Definitions

- [NonnegativeDecimal](#s-4e37479e1d)

##### <a id="s-4e37479e1d"></a>definition `NonnegativeDecimal`

- <a id="s-f7e692f58e"></a>`type`: `"string"`
- <a id="s-e9ceb88bdf"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-processingclaimrenewdocument-getitem.md)
- [get](riverhog-protocol-processingclaimrenewdocument-get.md)

## Governing policies

- <a id="pa-842e0b955a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimRenewDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b3bc6109aae14725ab7aa1a389920dd151a2aa7e1945fedb9a1dc4f208fa543 -->

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
        "fence": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
        },
        "lease_seconds": {
          "default": 1800,
          "maximum": 86400,
          "minimum": 30,
          "type": "integer"
        }
      },
      "required": [
        "fence"
      ],
      "type": "object"
    },
    "signature": "'(*, fence: Annotated[NonnegativeDecimal, Ge(ge=1)], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimRenewDocument",
  "unit": "export"
}
```

</details>
