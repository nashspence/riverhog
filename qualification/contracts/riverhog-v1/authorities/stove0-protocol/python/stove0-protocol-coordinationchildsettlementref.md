# stove0_protocol.CoordinationChildSettlementRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationchildsettlementref:7e90e7527f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-76d31e65e9"></a>
| Field | Shape |
|---|---|
| <a id="s-5f37398935"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5c837da626"></a>`distribution` | "stove0-protocol" |
| <a id="s-036e387cb3"></a>`module` | "stove0_protocol" |
| <a id="s-ed6d230550"></a>`name` | "CoordinationChildSettlementRef" |
| <a id="s-4e938f4980"></a>`unit` | "export" |

## Governing policies

- <a id="pa-6b18a22e70"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationChildSettlementRef`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b77fc9c9afd62445da1052371e04eead19708ac5d55aa8da24c00aa5c5299f41 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7bd31023cc09897af3321f85d7438ce508e1093e2abf51326d29362ab02bb869",
    "signature": "\"(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], kind: Literal['collection', 'external-effect', 'coordination'], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "CoordinationChildSettlementRef",
  "unit": "export"
}
```
