# riverhog_provenance_linux_contracts.load_schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-linux-contracts:riverhog-provenance-linux-contracts-load-schemas:7d5ec2d08c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-17d61e8d04"></a>
- <a id="s-749c21b6c9"></a>`distribution`: `riverhog-provenance-linux-contracts`
- <a id="s-ca50fd3008"></a>`module`: `riverhog_provenance_linux_contracts`
- <a id="s-b752b62bf3"></a>`name`: `load_schemas`
- <a id="s-738c8cdc70"></a>`unit`: `export`

### Declared structure

- <a id="s-3a10b28793"></a>`kind`: `"function"`
- <a id="s-e82c795041"></a>`signature`: `"\"() -> 'dict[str, dict[str, Any]]'\""`

## Governing policies

- <a id="pa-a3d8be7423"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-linux-contracts:riverhog_provenance_linux_contracts](../../../evidence/sources.md#src-2cb2292124) — `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_linux_contracts.load_schemas`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f814eda8e5cf49a3c8c0a4f0011a8947922c0f2cedf642c7a8d6a46a04997369 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'dict[str, dict[str, Any]]'\""
  },
  "distribution": "riverhog-provenance-linux-contracts",
  "module": "riverhog_provenance_linux_contracts",
  "name": "load_schemas",
  "unit": "export"
}
```

</details>
