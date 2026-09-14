# riverhog_protocol.SearchSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-searchsort:7ba5873b97 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5ea4436e90"></a>
- <a id="s-a050209fee"></a>`distribution`: `riverhog-protocol`
- <a id="s-9142397d61"></a>`module`: `riverhog_protocol`
- <a id="s-397a6540cb"></a>`name`: `SearchSort`
- <a id="s-2d0f5436c3"></a>`unit`: `export`

### Declared structure

- <a id="s-569dda1550"></a>`kind`: `"type-alias"`
- <a id="s-c206ed6e36"></a>`value`: `"typing.Literal['file_ref', 'collection_id', 'path', 'bytes']"`

## Governing policies

- <a id="pa-e42ae3240c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.SearchSort`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 20e960869be420f51c1a2551edd5ad55793489c84eb7bf2a662cc0dd317f716b -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['file_ref', 'collection_id', 'path', 'bytes']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "SearchSort",
  "unit": "export"
}
```
