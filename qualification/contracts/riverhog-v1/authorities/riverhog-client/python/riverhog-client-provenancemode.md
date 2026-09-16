# riverhog_client.ProvenanceMode

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-provenancemode:9f43dba464 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a72c0260da"></a>
- <a id="s-201b6cd9c7"></a>`distribution`: `riverhog-client`
- <a id="s-2748e81695"></a>`module`: `riverhog_client`
- <a id="s-61bbad0b3a"></a>`name`: `ProvenanceMode`
- <a id="s-2533c78dee"></a>`unit`: `export`

### Declared structure

- <a id="s-4fd3e1def1"></a>`kind`: `"type-alias"`
- <a id="s-2b2c4d7640"></a>`value`: `"typing.Literal['captured', 'omitted']"`

## Governing policies

- <a id="pa-c9e960aafb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ProvenanceMode`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42d816d7e0bdec54d0642fb25cbbf7841561ede2c466fc2edf44ba6edbe2983f -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['captured', 'omitted']"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ProvenanceMode",
  "unit": "export"
}
```

</details>
