# riverhog_provenance_contracts.SHA256_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-sha256-pattern:eba9bb4a86 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fb51822ffa"></a>
- <a id="s-19eba23c07"></a>`distribution`: `riverhog-provenance-contracts`
- <a id="s-6d0f6a5768"></a>`module`: `riverhog_provenance_contracts`
- <a id="s-4be48c492c"></a>`name`: `SHA256_PATTERN`
- <a id="s-258e86494c"></a>`unit`: `export`

### Declared structure

- <a id="s-5c458799bb"></a>`kind`: `"constant"`
- <a id="s-187ed96d3e"></a>`value`: `"^[0-9a-f]{64}$"`

## Governing policies

- <a id="pa-465000d3d4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources.md#src-9b6289a988) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.SHA256_PATTERN`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c5e2d999b7b22c93ed86b61f268f8352685eb201f01ea4fedd959f4684570f3 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[0-9a-f]{64}$"
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "SHA256_PATTERN",
  "unit": "export"
}
```

</details>
