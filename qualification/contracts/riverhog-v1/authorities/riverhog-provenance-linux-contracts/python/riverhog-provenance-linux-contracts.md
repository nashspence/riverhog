# riverhog_provenance_linux_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-linux-contracts:riverhog-provenance-linux-contracts:bfda10dc4b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-contracts](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4170e082cb"></a>
| Field | Shape |
|---|---|
| <a id="s-a854af9851"></a>`candidate_id` | "python:riverhog-provenance-linux-contracts:riverhog_provenance_linux_contracts" |
| <a id="s-ae0335d185"></a>`distribution` | "riverhog-provenance-linux-contracts" |
| <a id="s-50bd3ec469"></a>`exports` | additional keys=`CONTRACT_BINDING`, `CONTRACT_ID`, `PLATFORM_FAMILY`, `load_schemas` |
| <a id="s-f43a3c49e0"></a>`module` | "riverhog_provenance_linux_contracts" |

## Governing policies

- <a id="pa-92a88f9454"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-linux-contracts:riverhog_provenance_linux_contracts](../../../evidence/sources.md#src-2cb2292124) — `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/__init__.py`

### Machine authority

- `/external_contract/python/20`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1455bf536f4565b4d39f9facb12a0d6b2ccb2efe62aa45c71381fe1bcfcfaf0 -->

```json
{
  "candidate_id": "python:riverhog-provenance-linux-contracts:riverhog_provenance_linux_contracts",
  "distribution": "riverhog-provenance-linux-contracts",
  "exports": {
    "CONTRACT_BINDING": {
      "kind": "object",
      "type": "riverhog_provenance_contracts.ProvenanceContractBinding"
    },
    "CONTRACT_ID": {
      "kind": "constant",
      "value": "riverhog-provenance-linux-observation/v1"
    },
    "PLATFORM_FAMILY": {
      "kind": "constant",
      "value": "linux"
    },
    "load_schemas": {
      "kind": "function",
      "signature": "\"() -> 'dict[str, dict[str, Any]]'\""
    }
  },
  "module": "riverhog_provenance_linux_contracts"
}
```
