# riverhog_client.hash_raw_source_chunks

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-hash-raw-source-chunks:90f3be15c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-81d765d236"></a>
- <a id="s-ba2eaa2c61"></a>`distribution`: `riverhog-client`
- <a id="s-b9326d7fa6"></a>`module`: `riverhog_client`
- <a id="s-5fc523249a"></a>`name`: `hash_raw_source_chunks`
- <a id="s-12e5fc102e"></a>`unit`: `export`

### Declared structure

- <a id="s-566350f850"></a>`kind`: `"function"`
- <a id="s-5af733dd8b"></a>`signature`: `"\"(*, path: 'str', chunks: 'Iterable[bytes]', expected_bytes: 'int', part_plaintext_bytes: 'int') -> 'RawSourceHash'\""`

## Governing policies

- <a id="pa-3dbb0a03d3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.hash_raw_source_chunks`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44e7512c1b67847e126c012ee130bc2cdc58082bb96b1678e25fb2b1e41f57bf -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, path: 'str', chunks: 'Iterable[bytes]', expected_bytes: 'int', part_plaintext_bytes: 'int') -> 'RawSourceHash'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "hash_raw_source_chunks",
  "unit": "export"
}
```
