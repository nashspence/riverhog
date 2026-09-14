# stove0_target_protocol.TargetCallbackAccess

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcallbackaccess:292785ebcf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2aa91b22b2"></a>
- <a id="s-47a1aa2aa7"></a>`distribution`: `stove0-target-protocol`
- <a id="s-64db854d1a"></a>`module`: `stove0_target_protocol`
- <a id="s-f0b9de85dc"></a>`name`: `TargetCallbackAccess`
- <a id="s-84420f33f1"></a>`unit`: `export`

### Declared structure

- <a id="s-ed17947e7a"></a>`kind`: `"class"`
- <a id="s-ffe860150d"></a>`signature`: `"'(*, stove0_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False) -> None'"`

#### Validated model schema

<a id="s-0909029cad"></a>
- <a id="s-2be21a0dd3"></a>`title`: TargetCallbackAccess
- <a id="s-6e5b9d69f6"></a>`description`: Secret-bearing execution callback authority excluded from plan identity.
- <a id="s-d47353300f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f83edb9700"></a>`allow_insecure_http` | no | type="boolean" |  |
| <a id="s-f10ea78b70"></a>`stove0_base_url` | yes | type="string"; minLength=1; maxLength=2048 |  |
| <a id="s-eceb86407c"></a>`token` | yes | type="string"; minLength=1; maxLength=4096 |  |

## Governing policies

- <a id="pa-efb7bf6beb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetCallbackAccess`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1fa9f7073b986495e3ceebd20c8df8a8aecab46e7e536882b062e72334dc5432 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "description": "Secret-bearing execution callback authority excluded from plan identity.",
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "stove0_base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "title": "Stove0 Base Url",
          "type": "string"
        },
        "token": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Token",
          "type": "string"
        }
      },
      "required": [
        "stove0_base_url",
        "token"
      ],
      "title": "TargetCallbackAccess",
      "type": "object"
    },
    "signature": "'(*, stove0_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False) -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetCallbackAccess",
  "unit": "export"
}
```
