# stove0_core.TargetCallbackAuthority.process_due_production_seals

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetcallbackauthority-proce-a48dcb3894:0b5bd52ed5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-572679f538"></a>
- <a id="s-b5a62d54b4"></a>`distribution`: `stove0-server`
- <a id="s-c95fec8bd7"></a>`module`: `stove0_core`
- <a id="s-c43a37bfe2"></a>`name`: `process_due_production_seals`
- <a id="s-2e87b542b0"></a>`owner`: `stove0_core.TargetCallbackAuthority`
- <a id="s-c3d178d025"></a>`unit`: `member`

### Declared structure

- <a id="s-ba54374383"></a>`kind`: `"method"`
- <a id="s-a9ba9aed77"></a>`signature`: `"\"(self, *, limit: 'int' = 1) -> 'int'\""`

## Maintained corroboration

### Related interface records

- [TargetCallbackAuthority](stove0-core-targetcallbackauthority.md)

## Governing policies

- <a id="pa-89d6566864"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TargetCallbackAuthority.process_due_production_seals`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6beee3bdc0fe12aebf473fe53446e23a04c65902f6f257349822f3438ddc41dd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, limit: 'int' = 1) -> 'int'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "process_due_production_seals",
  "owner": "stove0_core.TargetCallbackAuthority",
  "unit": "member"
}
```

</details>
