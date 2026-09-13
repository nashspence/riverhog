# stove0_protocol.EvaluationVariant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationvariant:4a291e1f56 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d01ffa96c6"></a>
| Field | Shape |
|---|---|
| <a id="s-56019a589a"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d4f0a6951d"></a>`distribution` | "stove0-protocol" |
| <a id="s-a80fc059ed"></a>`module` | "stove0_protocol" |
| <a id="s-a49c748fad"></a>`name` | "EvaluationVariant" |
| <a id="s-7c85c8f655"></a>`unit` | "export" |

## Governing policies

- <a id="pa-2fda50e075"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationVariant`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e89d566d92a73a66a51755146d6a0be9e6b4b5d6dc556707cee0eb54c1cce660 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5cec278805e11bb3d57749f49b73debbcf8efbe5de4cb285d198578acb72bab1",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], parameters: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EvaluationVariant",
  "unit": "export"
}
```
