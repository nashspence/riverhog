# riverhog_protocol.CollectionTagSetRoot.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagsetroot-seal:f3963f6d54 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2d676597ee"></a>
- <a id="s-85b5a25b91"></a>`distribution`: `riverhog-protocol`
- <a id="s-ebc857942a"></a>`module`: `riverhog_protocol`
- <a id="s-6744819d75"></a>`name`: `seal`
- <a id="s-13f750876b"></a>`owner`: `riverhog_protocol.CollectionTagSetRoot`
- <a id="s-17ccfb486f"></a>`unit`: `member`

### Declared structure

- <a id="s-bedcc86ed3"></a>`kind`: `"classmethod"`
- <a id="s-3a443b288f"></a>`signature`: `"\"(cls, root_sha256: 'str \| None') -> 'CollectionTagSetRoot'\""`

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionTagSetRoot](riverhog-protocol-collectiontagsetroot.md)

## Governing policies

- <a id="pa-3578c5221c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagSetRoot.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: caedbc3b23d0720081deade736179058282f4bcc0e31942ae7b41b77b1e21a00 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, root_sha256: 'str | None') -> 'CollectionTagSetRoot'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "seal",
  "owner": "riverhog_protocol.CollectionTagSetRoot",
  "unit": "member"
}
```
