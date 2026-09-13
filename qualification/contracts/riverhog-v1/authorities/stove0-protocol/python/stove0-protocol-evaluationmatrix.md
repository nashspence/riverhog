# stove0_protocol.EvaluationMatrix

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationmatrix:54ed43c4cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2dcd6baa30"></a>
| Field | Shape |
|---|---|
| <a id="s-a8e95a3e3e"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-896760fd5b"></a>`distribution` | "stove0-protocol" |
| <a id="s-ae4c33ab8f"></a>`module` | "stove0_protocol" |
| <a id="s-064a110e4f"></a>`name` | "EvaluationMatrix" |
| <a id="s-2d539792ce"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.EvaluationMatrix.seal](stove0-protocol-evaluationmatrix-seal.md)
- [stove0_protocol.EvaluationMatrix.verify_digest](stove0-protocol-evaluationmatrix-verify-digest.md)

## Governing policies

- <a id="pa-7eb7f85086"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationMatrix`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3285df8eb64930ffebe016dbd7fe779754a0ea9b0786e63df9e75d9b44a2c8c3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "2d0bb04bdfeb8d3e6f9fab1b20a84df01677032f669043d7d567d690230c8960",
    "signature": "\"(*, format: Literal['stove0-evaluation-matrix/v1'] = 'stove0-evaluation-matrix/v1', variants: Annotated[tuple[stove0_protocol.models.EvaluationVariant, ...], MinLen(min_length=1)], matrix_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EvaluationMatrix",
  "unit": "export"
}
```
