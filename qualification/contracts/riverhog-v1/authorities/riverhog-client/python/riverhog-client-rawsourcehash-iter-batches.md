# riverhog_client.RawSourceHash.iter_batches

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-rawsourcehash-iter-batches:197800dcb9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b9ef461aa1"></a>
- <a id="s-320e9bbc5d"></a>`distribution`: `riverhog-client`
- <a id="s-8f41cf0771"></a>`module`: `riverhog_client`
- <a id="s-278e3330e9"></a>`name`: `iter_batches`
- <a id="s-10ce27d1e7"></a>`owner`: `riverhog_client.RawSourceHash`
- <a id="s-3da2697508"></a>`unit`: `member`

### Declared structure

- <a id="s-e6b41a5310"></a>`kind`: `"method"`
- <a id="s-19c34da3d4"></a>`signature`: `"\"(self, *, limit: 'int' = 1024) -> 'Iterator[tuple[int, tuple[str, ...]]]'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.RawSourceHash](riverhog-client-rawsourcehash.md)

## Governing policies

- <a id="pa-bd3b2d7b68"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.RawSourceHash.iter_batches`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c150ac7d77d28f3d2f5ac313e54d5da10b325b08e6f35b539fc976830d25d18f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, limit: 'int' = 1024) -> 'Iterator[tuple[int, tuple[str, ...]]]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "iter_batches",
  "owner": "riverhog_client.RawSourceHash",
  "unit": "member"
}
```
