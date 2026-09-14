# stove0_core.PreviewAcceptance.canonical_targets

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-previewacceptance-canonical-targets:3a65664084 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8bbfca6f45"></a>
- <a id="s-a2189fbae2"></a>`distribution`: `stove0-server`
- <a id="s-fc8b5d108d"></a>`module`: `stove0_core`
- <a id="s-8356efe349"></a>`name`: `canonical_targets`
- <a id="s-70dd47fd07"></a>`owner`: `stove0_core.PreviewAcceptance`
- <a id="s-7ba9ae7510"></a>`unit`: `member`

### Declared structure

- <a id="s-72257ad5be"></a>`kind`: `"method"`
- <a id="s-8607949754"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [PreviewAcceptance](stove0-core-previewacceptance.md)

## Governing policies

- <a id="pa-2d7d676ae9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.PreviewAcceptance.canonical_targets`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c285c0b522e0df789952cf4955854d66ad7ef96857b228c0eea4df2d3001656e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "canonical_targets",
  "owner": "stove0_core.PreviewAcceptance",
  "unit": "member"
}
```
