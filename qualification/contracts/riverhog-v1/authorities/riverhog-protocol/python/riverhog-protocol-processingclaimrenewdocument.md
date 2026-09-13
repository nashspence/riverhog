# riverhog_protocol.ProcessingClaimRenewDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimrenewdocument:55743c6830 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67d8290270"></a>
| Field | Shape |
|---|---|
| <a id="s-83488c7314"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-409aa45f28"></a>`distribution` | "riverhog-protocol" |
| <a id="s-ae7fd162ea"></a>`module` | "riverhog_protocol" |
| <a id="s-a6c8ba6b13"></a>`name` | "ProcessingClaimRenewDocument" |
| <a id="s-e58af8205b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-842e0b955a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimRenewDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f299dd634036fe627de756321c8a45f6f6c26c09ca4c6f7e085c9f251a65ecd5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "653180bef69a08ae3179fd94db145a2e362fe71ddfe0e1bbfd4e65e8ced1f146",
    "signature": "'(*, fence: Annotated[int, Ge(ge=1)], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimRenewDocument",
  "unit": "export"
}
```
