# stove0_observer_protocol.ObserverRuntimeAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerruntimeauthority:8689031ba5 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-7986f72e0a"></a>`title`: ObserverRuntimeAuthority
- <a id="s-8b823ba8ff"></a>`description`: Secret-bearing invocation material excluded from durable request identity.
- <a id="s-5bffbb9ea1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8fea69c4e1"></a>`allow_insecure_http` | no | type="boolean" |  |
| <a id="s-0d713c3e25"></a>`capability_token` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-da7dd7d153"></a>`riverhog_base_url` | yes | type="string"; minLength=1; maxLength=2048 |  |
| <a id="s-4e7f4eced5"></a>`transport` | no | type="string"; const="riverhog-capability/v1" |  |
| <a id="s-f18e206884"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"] |  |

## Governing policies

- <a id="pa-8aea2907d7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverRuntimeAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91cf4de711f6812e79d7fb88ce76f7180953d9da7c93327c37420b75080a4ee3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "description": "Secret-bearing invocation material excluded from durable request identity.",
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "capability_token": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Capability Token",
          "type": "string"
        },
        "riverhog_base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "title": "Riverhog Base Url",
          "type": "string"
        },
        "transport": {
          "const": "riverhog-capability/v1",
          "default": "riverhog-capability/v1",
          "title": "Transport",
          "type": "string"
        },
        "workspace_assurance": {
          "enum": [
            "encrypted",
            "ephemeral"
          ],
          "title": "Workspace Assurance",
          "type": "string"
        }
      },
      "required": [
        "riverhog_base_url",
        "capability_token",
        "workspace_assurance"
      ],
      "title": "ObserverRuntimeAuthority",
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
