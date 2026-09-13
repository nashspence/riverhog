# riverhog_protocol.ProcessingClaimRestartDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimrestartdocument:689c762e66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-76c82fb3d3"></a>
| Field | Shape |
|---|---|
| <a id="s-fb38e5f66f"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ef188b1b4d"></a>`distribution` | "riverhog-protocol" |
| <a id="s-a02774a774"></a>`module` | "riverhog_protocol" |
| <a id="s-e3d4ec07f0"></a>`name` | "ProcessingClaimRestartDocument" |
| <a id="s-1db7fb9246"></a>`unit` | "export" |

## Governing policies

- <a id="pa-ff86f2192f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimRestartDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 81dc2ddfb955dce112073325602bf1e42fb611e10fc37d29849f9e068e7cbc68 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a1567d5ffa38a300b1cbcb6ae7cf2fc435dc07e4f6a6b18d2578795929f17619",
    "signature": "'(*, fence: Annotated[int, Ge(ge=1)], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimRestartDocument",
  "unit": "export"
}
```
