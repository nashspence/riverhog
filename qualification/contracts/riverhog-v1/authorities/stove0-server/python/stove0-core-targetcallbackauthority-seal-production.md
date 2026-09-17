# stove0_core.TargetCallbackAuthority.seal_production

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetcallbackauthority-seal-production:f9b05593d2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-309b0a4b5b"></a>
- <a id="s-1687631d71"></a>`distribution`: `stove0-server`
- <a id="s-570c0acc25"></a>`module`: `stove0_core`
- <a id="s-6b673050b9"></a>`name`: `seal_production`
- <a id="s-4223fc2529"></a>`owner`: `stove0_core.TargetCallbackAuthority`
- <a id="s-547ba78907"></a>`unit`: `member`

### Declared structure

- <a id="s-3f339ccc1b"></a>`kind`: `"method"`
- <a id="s-139948f534"></a>`signature`: `"\"(self, token: 'str', *, job_id: 'str') -> 'TargetProductionSealResponse'\""`

## Maintained corroboration

### Related interface records

- [TargetCallbackAuthority](stove0-core-targetcallbackauthority.md)

## Governing policies

- <a id="pa-7aa863ee47"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TargetCallbackAuthority.seal_production`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0acba1f795983fe22565a2656b2aebd761975c33ac7b340414db041e87ca358 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, token: 'str', *, job_id: 'str') -> 'TargetProductionSealResponse'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "seal_production",
  "owner": "stove0_core.TargetCallbackAuthority",
  "unit": "member"
}
```

</details>
