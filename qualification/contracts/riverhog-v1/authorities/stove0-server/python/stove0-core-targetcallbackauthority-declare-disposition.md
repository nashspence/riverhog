# stove0_core.TargetCallbackAuthority.declare_disposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetcallbackauthority-decla-322b4adc6d:0a9ac3e393 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c1d335340d"></a>
- <a id="s-03783554ef"></a>`distribution`: `stove0-server`
- <a id="s-fce7f50789"></a>`module`: `stove0_core`
- <a id="s-bbf8053443"></a>`name`: `declare_disposition`
- <a id="s-5fc315bdc6"></a>`owner`: `stove0_core.TargetCallbackAuthority`
- <a id="s-f4d0cefd0d"></a>`unit`: `member`

### Declared structure

- <a id="s-24da2139fb"></a>`kind`: `"method"`
- <a id="s-e490d6838d"></a>`signature`: `"\"(self, token: 'str', *, job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TargetCallbackAuthority](stove0-core-targetcallbackauthority.md)

## Governing policies

- <a id="pa-677ef3b3e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.TargetCallbackAuthority.declare_disposition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e55d7b04c9244d68dd22ab68983996f7dde998d32eb792b62ad35dcdc25ba888 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, token: 'str', *, job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "declare_disposition",
  "owner": "stove0_core.TargetCallbackAuthority",
  "unit": "member"
}
```
