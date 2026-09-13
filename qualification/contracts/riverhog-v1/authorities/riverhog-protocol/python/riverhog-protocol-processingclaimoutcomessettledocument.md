# riverhog_protocol.ProcessingClaimOutcomesSettleDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimoutcomes-ced5cc25fd:edc78d3351 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-86ef45c1c4"></a>
| Field | Shape |
|---|---|
| <a id="s-24cb639c5c"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c38840d457"></a>`distribution` | "riverhog-protocol" |
| <a id="s-4a37585a48"></a>`module` | "riverhog_protocol" |
| <a id="s-702b52e103"></a>`name` | "ProcessingClaimOutcomesSettleDocument" |
| <a id="s-7a6fcbbcd5"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ProcessingClaimOutcomesSettleDocument.validate_outcomes](riverhog-protocol-processingclaimoutcomessettledocument-validate-outcomes.md)

## Governing policies

- <a id="pa-19e6f7fcd1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimOutcomesSettleDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 01e3b326263c31ab22f6b31e5099fc8c4ff26c2563c59b44d51554f4eafccfa7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "52b850839b3d9d5f6dfb73fc055199f04030f6a5c9294298c368ae3bf1550d69",
    "signature": "\"(*, fence: Annotated[int, Ge(ge=1)], retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimOutcomesSettleDocument",
  "unit": "export"
}
```
