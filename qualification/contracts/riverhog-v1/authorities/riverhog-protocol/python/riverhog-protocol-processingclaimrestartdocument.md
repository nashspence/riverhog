# riverhog_protocol.ProcessingClaimRestartDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimrestartdocument:689c762e66 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-76c82fb3d3"></a>
- <a id="s-ef188b1b4d"></a>`distribution`: `riverhog-protocol`
- <a id="s-a02774a774"></a>`module`: `riverhog_protocol`
- <a id="s-e3d4ec07f0"></a>`name`: `ProcessingClaimRestartDocument`
- <a id="s-1db7fb9246"></a>`unit`: `export`

### Declared structure

- <a id="s-e6338ea845"></a>`kind`: `"class"`
- <a id="s-c5be82a6f1"></a>`signature`: `"'(*, fence: Annotated[NonnegativeDecimal, Ge(ge=1)], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800) -> None'"`

#### Validated model schema

<a id="s-445f8600e0"></a>

- <a id="s-a2aaf7a807"></a>`type`: `"object"`
- <a id="s-20eceda319"></a>`additionalProperties`: `false`
- <a id="s-f43e4113dd"></a>`required`: `["fence"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1f8e290dab"></a>`fence` | yes | [NonnegativeDecimal](#s-c555f2332b); ge=1 |  |
| <a id="s-0833ed1442"></a>`lease_seconds` | no | type="integer"; minimum=30; maximum=86400; default=1800 |  |

##### Definitions

- [NonnegativeDecimal](#s-c555f2332b)

##### <a id="s-c555f2332b"></a>definition `NonnegativeDecimal`

- <a id="s-5510370069"></a>`type`: `"string"`
- <a id="s-66a9c00449"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-processingclaimrestartdocument-getitem.md)
- [get](riverhog-protocol-processingclaimrestartdocument-get.md)

## Governing policies

- <a id="pa-ff86f2192f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimRestartDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b87bdccae28fe9d67b4da8096373d459d428ea0012bcd9edd08732ef6021a47d -->

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
  "name": "ProcessingClaimRestartDocument",
  "unit": "export"
}
```

</details>
