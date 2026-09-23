# riverhog_protocol.ProcessingClaimFenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimfencedocument:aebabca317 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7e9c3fb5e0"></a>
- <a id="s-d774b48fcc"></a>`distribution`: `riverhog-protocol`
- <a id="s-20f0f1e79b"></a>`module`: `riverhog_protocol`
- <a id="s-145e9046b9"></a>`name`: `ProcessingClaimFenceDocument`
- <a id="s-153b43a728"></a>`unit`: `export`

### Declared structure

- <a id="s-6e4f5a087f"></a>`kind`: `"class"`
- <a id="s-fd249713d4"></a>`signature`: `"'(*, fence: Annotated[NonnegativeDecimal, Ge(ge=1)]) -> None'"`

#### Validated model schema

<a id="s-79182f253c"></a>

- <a id="s-e838297b11"></a>`type`: `"object"`
- <a id="s-434a0236a8"></a>`additionalProperties`: `false`
- <a id="s-b9ce4aad9e"></a>`required`: `["fence"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9eaab391aa"></a>`fence` | yes | [NonnegativeDecimal](#s-3bf82409e2); ge=1 |  |

##### Definitions

- [NonnegativeDecimal](#s-3bf82409e2)

##### <a id="s-3bf82409e2"></a>definition `NonnegativeDecimal`

- <a id="s-d20e42f75c"></a>`type`: `"string"`
- <a id="s-7e14f160e1"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-processingclaimfencedocument-getitem.md)
- [get](riverhog-protocol-processingclaimfencedocument-get.md)

## Governing policies

- <a id="pa-530b1c7343"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimFenceDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b202107050d1c57f9d849ec9e00030fdb8d02d15a3bdf3bdf9d4b2602acbfcd6 -->

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
        }
      },
      "required": [
        "fence"
      ],
      "type": "object"
    },
    "signature": "'(*, fence: Annotated[NonnegativeDecimal, Ge(ge=1)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimFenceDocument",
  "unit": "export"
}
```

</details>
