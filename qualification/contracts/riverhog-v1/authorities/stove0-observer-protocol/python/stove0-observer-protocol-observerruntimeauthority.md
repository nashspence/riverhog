# stove0_observer_protocol.ObserverRuntimeAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerruntimeauthority:8689031ba5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-70b1c3ab83"></a>
- <a id="s-587c69aeaf"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-058c5aa9e6"></a>`module`: `stove0_observer_protocol`
- <a id="s-d4dd2596f3"></a>`name`: `ObserverRuntimeAuthority`
- <a id="s-b29503ad59"></a>`unit`: `export`

### Declared structure

- <a id="s-4b7cae6005"></a>`kind`: `"class"`
- <a id="s-0a14d48ee4"></a>`signature`: `"\"(*, transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], capability_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False, workspace_assurance: Literal['encrypted', 'ephemeral']) -> None\""`

#### Validated model schema

<a id="s-275dff45e9"></a>

- <a id="s-5bffbb9ea1"></a>`type`: `"object"`
- <a id="s-c02fd19ae5"></a>`additionalProperties`: `false`
- <a id="s-31056a8ba9"></a>`required`: `["riverhog_base_url","capability_token","workspace_assurance"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8fea69c4e1"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-0d713c3e25"></a>`capability_token` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-da7dd7d153"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-4e7f4eced5"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |
| <a id="s-f18e206884"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"] |  |

## Governing policies

- <a id="pa-8aea2907d7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverRuntimeAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3297c571aab2e07b9e7ee968589ba6bd437be3a4d0770d93e1a12fe2ce59893d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "type": "boolean"
        },
        "capability_token": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "riverhog_base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "type": "string"
        },
        "transport": {
          "const": "riverhog-capability/v1",
          "default": "riverhog-capability/v1",
          "type": "string"
        },
        "workspace_assurance": {
          "enum": [
            "encrypted",
            "ephemeral"
          ],
          "type": "string"
        }
      },
      "required": [
        "riverhog_base_url",
        "capability_token",
        "workspace_assurance"
      ],
      "type": "object"
    },
    "signature": "\"(*, transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], capability_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False, workspace_assurance: Literal['encrypted', 'ephemeral']) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverRuntimeAuthority",
  "unit": "export"
}
```

</details>
