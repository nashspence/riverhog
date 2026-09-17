# riverhog_provenance_contracts.PROVENANCE_CONTRACT_REFERENCE_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-provenance-f5cf42227c:855e65a1fd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-90da8130e5"></a>
- <a id="s-ce1a571d81"></a>`distribution`: `riverhog-provenance-contracts`
- <a id="s-55bca0246f"></a>`module`: `riverhog_provenance_contracts`
- <a id="s-4e232dc671"></a>`name`: `PROVENANCE_CONTRACT_REFERENCE_FORMAT`
- <a id="s-ff67d254e2"></a>`unit`: `export`

### Declared structure

- <a id="s-429bcc520e"></a>`kind`: `"constant"`
- <a id="s-83f21b27ab"></a>`value`: `"riverhog-provenance-contract-reference/v1"`

## Governing policies

- <a id="pa-798a64e531"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources/authorities.md#src-9b6289a988) — [packages/riverhog-provenance-contracts/src/riverhog\_provenance\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.PROVENANCE_CONTRACT_REFERENCE_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b99df2f8813741fd5cc2defe9d4db7171d5fca07c5d0a40ccb11de019fa8bf9 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-contract-reference/v1"
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "PROVENANCE_CONTRACT_REFERENCE_FORMAT",
  "unit": "export"
}
```

</details>
