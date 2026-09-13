# stove0_protocol.OperationRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-operationref:73ef6dc3ba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-11d7e1b519"></a>
| Field | Shape |
|---|---|
| <a id="s-0adcc4a468"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d6cc25febe"></a>`distribution` | "stove0-protocol" |
| <a id="s-660336acf1"></a>`module` | "stove0_protocol" |
| <a id="s-a5b2bcbad6"></a>`name` | "OperationRef" |
| <a id="s-0ab4d35b2d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.OperationRef.from_identity](stove0-protocol-operationref-from-identity.md)
- [stove0_protocol.OperationRef.to_identity](stove0-protocol-operationref-to-identity.md)

## Governing policies

- <a id="pa-45aad26098"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.OperationRef`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 852e013cc118f50eaabf1e651a5504fd9b750c6a916805c25b6362b6224e2ecd -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "b0a71e15638b56b26ba692329ca73014bfa5bf6026623cec421455689c8542f8",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "OperationRef",
  "unit": "export"
}
```
