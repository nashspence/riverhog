# riverhog_protocol.ImmutableFileIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-immutablefileidentitydocument:d5464de65b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f1bd22a3d6"></a>
- <a id="s-99a9e71ff8"></a>`distribution`: `riverhog-protocol`
- <a id="s-aeced36ffc"></a>`module`: `riverhog_protocol`
- <a id="s-d58d184434"></a>`name`: `ImmutableFileIdentityDocument`
- <a id="s-c4a485c480"></a>`unit`: `export`

### Declared structure

- <a id="s-7700da86eb"></a>`kind`: `"class"`
- <a id="s-9c54513f5e"></a>`signature`: `"\"(*, path: CanonicalRelPath, bytes: NonnegativeDecimal, sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-6c90ef496b"></a>

- <a id="s-a26b9a0466"></a>`type`: `"object"`
- <a id="s-70f2b7f9f2"></a>`additionalProperties`: `false`
- <a id="s-e834a73fc1"></a>`required`: `["path","bytes","sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-73aa3462b8"></a>`bytes` | yes | [NonnegativeDecimal](#s-fcac609203) |  |
| <a id="s-d8d372ba45"></a>`path` | yes | [CanonicalRelPath](#s-2f866a5e07) |  |
| <a id="s-e161f15c80"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CanonicalRelPath](#s-2f866a5e07)
- [NonnegativeDecimal](#s-fcac609203)

##### <a id="s-2f866a5e07"></a>definition `CanonicalRelPath`

- <a id="s-14f96c6e18"></a>`type`: `"string"`
- <a id="s-16e5868dcd"></a>`format`: `"riverhog-canonical-relpath-v1"`
- <a id="s-7410b951d5"></a>`maxLength`: `4096`
- <a id="s-e6eace92e9"></a>`minLength`: `1`
- <a id="s-b26210e9ee"></a>`pattern`: `"^[^/\\\\]+(?:/[^/\\\\]+)*$"`
- <a id="s-9a31e18b7d"></a>`x-unicode-normalization`: `"NFC"`

###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-3abb6d9b12"></a>1 | not=(pattern="(?:^\|/)\\.{1,2}(?:/\|$)") |
| <a id="s-5d4898801b"></a>2 | not=(pattern="^\\s\|\\s$") |
| <a id="s-cd50a44de7"></a>3 | not=(pattern="\\u0000") |
| <a id="s-bb19901f13"></a>4 | not=(pattern="[\\ud800-\\udfff]") |

##### <a id="s-fcac609203"></a>definition `NonnegativeDecimal`

- <a id="s-30e0786c14"></a>`type`: `"string"`
- <a id="s-0bcb075c3f"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Governing policies

- <a id="pa-8aa7427881"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ImmutableFileIdentityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f2fb3a362ba384698b1f612caa1233bb2fd8e0dd98eace63cf3e11713b537c8c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CanonicalRelPath": {
          "allOf": [
            {
              "not": {
                "pattern": "(?:^|/)\\.{1,2}(?:/|$)"
              }
            },
            {
              "not": {
                "pattern": "^\\s|\\s$"
              }
            },
            {
              "not": {
                "pattern": "\\u0000"
              }
            },
            {
              "not": {
                "pattern": "[\\ud800-\\udfff]"
              }
            }
          ],
          "format": "riverhog-canonical-relpath-v1",
          "maxLength": 4096,
          "minLength": 1,
          "pattern": "^[^/\\\\]+(?:/[^/\\\\]+)*$",
          "type": "string",
          "x-unicode-normalization": "NFC"
        },
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        "path": {
          "$ref": "#/$defs/CanonicalRelPath"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "path",
        "bytes",
        "sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, path: CanonicalRelPath, bytes: NonnegativeDecimal, sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ImmutableFileIdentityDocument",
  "unit": "export"
}
```

</details>
