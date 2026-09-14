# stove0_core.TargetCallbackAuthority.input_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetcallbackauthority-input-page:8906bd63a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f442e2e5ae"></a>
- <a id="s-b903b852ca"></a>`distribution`: `stove0-server`
- <a id="s-3500cd78fd"></a>`module`: `stove0_core`
- <a id="s-612c30f081"></a>`name`: `input_page`
- <a id="s-d464e4221c"></a>`owner`: `stove0_core.TargetCallbackAuthority`
- <a id="s-ff5fcceca9"></a>`unit`: `member`

### Declared structure

- <a id="s-b32118fc61"></a>`kind`: `"method"`
- <a id="s-112c6f113d"></a>`signature`: `"\"(self, token: 'str', *, job_id: 'str', continuation: 'str \| None', limit: 'int') -> 'TargetInputPage'\""`

## Maintained corroboration

### Related interface records

- [TargetCallbackAuthority](stove0-core-targetcallbackauthority.md)

## Governing policies

- <a id="pa-80e17442a8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.TargetCallbackAuthority.input_page`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f0fad0a4de70f66be874a188ec6bbf809d5ad85f552e21d48c8d3590e49525f9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, token: 'str', *, job_id: 'str', continuation: 'str | None', limit: 'int') -> 'TargetInputPage'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "input_page",
  "owner": "stove0_core.TargetCallbackAuthority",
  "unit": "member"
}
```
