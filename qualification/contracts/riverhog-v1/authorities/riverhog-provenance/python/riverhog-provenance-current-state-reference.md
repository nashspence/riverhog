# riverhog_provenance.current_state_reference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-current-state-reference:b7fdfaeda1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a2ee3a0045"></a>
- <a id="s-236177f7e9"></a>`distribution`: `riverhog-provenance`
- <a id="s-b6c961a9b2"></a>`module`: `riverhog_provenance`
- <a id="s-7912ae23c6"></a>`name`: `current_state_reference`
- <a id="s-1e568abafa"></a>`unit`: `export`

### Declared structure

- <a id="s-a661478561"></a>`kind`: `"function"`
- <a id="s-ed79478b87"></a>`signature`: `"\"(content: 'bytes') -> 'ExternalStateReference'\""`

## Governing policies

- <a id="pa-2f5f812548"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.current_state_reference`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1b96e6336cce4888fdb0c39c06cd43974b9fefffe945d5fd6009438087686adc -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(content: 'bytes') -> 'ExternalStateReference'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "current_state_reference",
  "unit": "export"
}
```

</details>
