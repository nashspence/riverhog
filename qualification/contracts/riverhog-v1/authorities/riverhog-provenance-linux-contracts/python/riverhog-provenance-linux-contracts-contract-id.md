# riverhog_provenance_linux_contracts.CONTRACT_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-linux-contracts:riverhog-provenance-linux-contracts-contract-id:17e510e299 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-40a2ec538c"></a>
- <a id="s-60ed997b81"></a>`distribution`: `riverhog-provenance-linux-contracts`
- <a id="s-1a9b30a936"></a>`module`: `riverhog_provenance_linux_contracts`
- <a id="s-6cb63316fb"></a>`name`: `CONTRACT_ID`
- <a id="s-7a5d9c92f9"></a>`unit`: `export`

### Declared structure

- <a id="s-51bbfca2cf"></a>`kind`: `"constant"`
- <a id="s-c853975aa2"></a>`value`: `"riverhog-provenance-linux-observation/v1"`

## Governing policies

- <a id="pa-abd201c33f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance-linux-contracts:riverhog_provenance_linux_contracts](../../../evidence/sources.md#src-2cb2292124) — [reference/riverhog/provenance/contracts/linux/src/riverhog\_provenance\_linux\_contracts/\_\_init\_\_.py](../../../../../../reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance_linux_contracts.CONTRACT_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec9643ba9365dc72c84e1491ba101291494843f1f3e637ae6245d248845a2956 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-linux-observation/v1"
  },
  "distribution": "riverhog-provenance-linux-contracts",
  "module": "riverhog_provenance_linux_contracts",
  "name": "CONTRACT_ID",
  "unit": "export"
}
```

</details>
