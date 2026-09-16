# riverhog_provenance_contracts.CANONICAL_UUID_URN_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-canonical-u-2cb75a7308:a07d79aea8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ab6141c3e1"></a>
- <a id="s-43de91ad64"></a>`distribution`: `riverhog-provenance-contracts`
- <a id="s-e5faea0af1"></a>`module`: `riverhog_provenance_contracts`
- <a id="s-0768210745"></a>`name`: `CANONICAL_UUID_URN_PATTERN`
- <a id="s-2705ab5607"></a>`unit`: `export`

### Declared structure

- <a id="s-643bf7e09c"></a>`kind`: `"constant"`
- <a id="s-e2145633e6"></a>`value`: `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"`

## Governing policies

- <a id="pa-d5207e128f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources.md#src-9b6289a988) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.CANONICAL_UUID_URN_PATTERN`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d698f9a061e0f53bc37702b33cfacbbbd6e058d0b13e258528ac59a69da4bdf2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "CANONICAL_UUID_URN_PATTERN",
  "unit": "export"
}
```

</details>
