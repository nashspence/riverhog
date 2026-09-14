# stove0_target_protocol.TargetOutputBindingSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetoutputbindin-358a0eb8dd:68283ecc96 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6cd1d09725"></a>
- <a id="s-bb2688b0a3"></a>`distribution`: `stove0-target-protocol`
- <a id="s-1420fa1540"></a>`module`: `stove0_target_protocol`
- <a id="s-6d3ed8d28e"></a>`name`: `TargetOutputBindingSetIdentity`
- <a id="s-ec6b4f922e"></a>`unit`: `export`

### Declared structure

- <a id="s-76df4717bf"></a>`kind`: `"class"`
- <a id="s-aeeac4ce25"></a>`signature`: `"\"(*, artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-2643fa4f62"></a>
- <a id="s-2fac1d1549"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-06360413e6"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-8d5abdd79a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fc9b3f7cc0"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-42b13c6d70"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetOutputBindingSetIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de0bb960f47d11059d0486d0600c6f20de373271d7167a5b26c292ceb85ed259 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "artifact_count": {
          "minimum": 1,
          "type": "integer"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "total_bytes": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "artifact_count",
        "total_bytes",
        "sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetOutputBindingSetIdentity",
  "unit": "export"
}
```
