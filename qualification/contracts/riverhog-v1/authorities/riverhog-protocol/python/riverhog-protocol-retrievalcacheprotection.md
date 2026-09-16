# riverhog_protocol.RetrievalCacheProtection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalcacheprotection:1a5abfe104 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dced3020e2"></a>
- <a id="s-162f66de3d"></a>`distribution`: `riverhog-protocol`
- <a id="s-4043b93880"></a>`module`: `riverhog_protocol`
- <a id="s-f208718d30"></a>`name`: `RetrievalCacheProtection`
- <a id="s-d7b70938d7"></a>`unit`: `export`

### Declared structure

- <a id="s-ae4cfde12c"></a>`kind`: `"type-alias"`
- <a id="s-c43ac5ec4a"></a>`value`: `"typing.Literal['protected', 'unleased']"`

## Governing policies

- <a id="pa-2b6f544682"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalCacheProtection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f7f3a0bf75dfcaa9248ff367b0e4f970b1f61cdf3de4e2a206b00f19ccef139d -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['protected', 'unleased']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetrievalCacheProtection",
  "unit": "export"
}
```

</details>
