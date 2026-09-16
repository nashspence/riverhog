# riverhog_provenance.PROVENANCE_ROOT_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-root-schema:64a7df5517 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4103d58e7b"></a>
- <a id="s-7ca531f07c"></a>`distribution`: `riverhog-provenance`
- <a id="s-0dd4aab61a"></a>`module`: `riverhog_provenance`
- <a id="s-3728b568d5"></a>`name`: `PROVENANCE_ROOT_SCHEMA`
- <a id="s-b543e19e52"></a>`unit`: `export`

### Declared structure

- <a id="s-d2615dac07"></a>`kind`: `"constant"`
- <a id="s-afce8b9809"></a>`value`: `"riverhog-provenance-root/v1"`

## Governing policies

- <a id="pa-510b1311a4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_ROOT_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37252aeb234ca5355d8a98827e6104f84994a9947b35107efd77db28205806d2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-root/v1"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_ROOT_SCHEMA",
  "unit": "export"
}
```

</details>
