# riverhog_protocol.CollectionTagSet.iter_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagset-iter-tags:6cc6bed016 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2c37e25144"></a>
- <a id="s-89c10b1a47"></a>`distribution`: `riverhog-protocol`
- <a id="s-205db9d6e9"></a>`module`: `riverhog_protocol`
- <a id="s-38bdc011ef"></a>`name`: `iter_tags`
- <a id="s-823896bd9c"></a>`owner`: `riverhog_protocol.CollectionTagSet`
- <a id="s-ad5a3dc6d9"></a>`unit`: `member`

### Declared structure

- <a id="s-a379134816"></a>`kind`: `"method"`
- <a id="s-ceb6596e62"></a>`signature`: `"\"(self, *, start_after_sha256: 'str \| None' = None) -> 'Iterator[str]'\""`

## Maintained corroboration

### Related interface records

- [CollectionTagSet](riverhog-protocol-collectiontagset.md)

## Governing policies

- <a id="pa-aa6743c7cc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagSet.iter_tags`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eaa706e9d2582da7f86c5ab0b4e3cde30ed0ac0dd5bf058183a9d5347b8de39d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, start_after_sha256: 'str | None' = None) -> 'Iterator[str]'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "iter_tags",
  "owner": "riverhog_protocol.CollectionTagSet",
  "unit": "member"
}
```
