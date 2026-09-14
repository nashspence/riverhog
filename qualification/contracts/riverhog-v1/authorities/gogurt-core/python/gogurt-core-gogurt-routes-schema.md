# gogurt_core.GOGURT_ROUTES_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurt-routes-schema:0068b95885 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-91982d2eea"></a>
- <a id="s-cd235c7262"></a>`distribution`: `gogurt-core`
- <a id="s-e97f45517b"></a>`module`: `gogurt_core`
- <a id="s-11cd9f5d58"></a>`name`: `GOGURT_ROUTES_SCHEMA`
- <a id="s-83bd343d54"></a>`unit`: `export`

### Declared structure

- <a id="s-fe45923281"></a>`kind`: `"constant"`
- <a id="s-853591b01f"></a>`value`: `{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"kind":{"const":"gogurt.routes","type":"string"},"routes":{"additionalProperties":{"additionalProperties":false,"properties":{"command":{"items":{"minLength":1,"type":"string"},"minItems":1,"type":"array"},"enabled":{"type":"boolean"}},"required":["command"],"type":"object"},"propertyNames":{"pattern":"^[a-z0-9]&#40;?:[a-z0-9-]{0,61}[a-z0-9])?$"},"type":"object"},"schema_version":{"const":1,"type":"integer"}},"required":["schema_version","kind","routes"],"type":"object"}`

## Governing policies

- <a id="pa-b5ef49634a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.GOGURT_ROUTES_SCHEMA`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9210679c66629f5c38f4b9e2c130bc77c48bb7189b6c4175ef12482480825644 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "additionalProperties": false,
      "properties": {
        "kind": {
          "const": "gogurt.routes",
          "type": "string"
        },
        "routes": {
          "additionalProperties": {
            "additionalProperties": false,
            "properties": {
              "command": {
                "items": {
                  "minLength": 1,
                  "type": "string"
                },
                "minItems": 1,
                "type": "array"
              },
              "enabled": {
                "type": "boolean"
              }
            },
            "required": [
              "command"
            ],
            "type": "object"
          },
          "propertyNames": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9-]{0,61}[a-z0-9])?$"
          },
          "type": "object"
        },
        "schema_version": {
          "const": 1,
          "type": "integer"
        }
      },
      "required": [
        "schema_version",
        "kind",
        "routes"
      ],
      "type": "object"
    }
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GOGURT_ROUTES_SCHEMA",
  "unit": "export"
}
```
