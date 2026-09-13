# riverhog_provenance_windows_contracts.CONTRACT_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-windows-contracts:riverhog-provenance-windows-contracts-contract-id:fbe6f9271b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-29c5f89b34"></a>
| Field | Shape |
|---|---|
| <a id="s-2c686fed30"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-c33ba95414"></a>`distribution` | "riverhog-provenance-windows-contracts" |
| <a id="s-b9afaa3b56"></a>`module` | "riverhog_provenance_windows_contracts" |
| <a id="s-e85b9960b3"></a>`name` | "CONTRACT_ID" |
| <a id="s-e2d11f964f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-42768af906"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-windows-contracts:riverhog_provenance_windows_contracts](../../../evidence/sources.md#src-0d5e61922a) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_windows_contracts.CONTRACT_ID`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 54da606c3ce7f9c99b2a768ba9529ffd3018d468897c17bf78bdc34f58cfa97d -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-windows-observation/v1"
  },
  "distribution": "riverhog-provenance-windows-contracts",
  "module": "riverhog_provenance_windows_contracts",
  "name": "CONTRACT_ID",
  "unit": "export"
}
```
