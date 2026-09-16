# riverhog_protocol.COLLECTION_FINALIZED

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-finalized:26fb91a267 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fdae78cc64"></a>
- <a id="s-2e0054618b"></a>`distribution`: `riverhog-protocol`
- <a id="s-3312fe6c6b"></a>`module`: `riverhog_protocol`
- <a id="s-5125e0ecad"></a>`name`: `COLLECTION_FINALIZED`
- <a id="s-441e96397c"></a>`unit`: `export`

### Declared structure

- <a id="s-3cefd48b40"></a>`kind`: `"constant"`
- <a id="s-870d0c1cce"></a>`value`: `"io.riverhog.riverhog.collection.finalized"`

## Governing policies

- <a id="pa-2dcfb308ec"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.COLLECTION_FINALIZED`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7215d582f6463b15d3ba546bd4e56f9999318653e3fb89b512c3f7b9cc03dd24 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "io.riverhog.riverhog.collection.finalized"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "COLLECTION_FINALIZED",
  "unit": "export"
}
```

</details>
