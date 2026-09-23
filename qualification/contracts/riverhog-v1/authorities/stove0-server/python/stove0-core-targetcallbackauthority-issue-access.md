# stove0_core.TargetCallbackAuthority.issue_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetcallbackauthority-issue-access:1ef24f6580 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-168507c8e9"></a>
- <a id="s-c4a3890349"></a>`distribution`: `stove0-server`
- <a id="s-b1d0bc9096"></a>`module`: `stove0_core`
- <a id="s-e56e5a42b7"></a>`name`: `issue_access`
- <a id="s-0dc11d206d"></a>`owner`: `stove0_core.TargetCallbackAuthority`
- <a id="s-242d57040c"></a>`unit`: `member`

### Declared structure

- <a id="s-5a7100f687"></a>`kind`: `"method"`
- <a id="s-93fb0fdb91"></a>`signature`: `"\"(self, record: 'WorkRecord', target_registration_id: 'str') -> 'TargetCallbackAccess'\""`

## Maintained corroboration

### Related interface records

- [TargetCallbackAuthority](stove0-core-targetcallbackauthority.md)

## Governing policies

- <a id="pa-e9d0c1eba1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TargetCallbackAuthority.issue_access`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39d3a7476cc293b6e957e8778797b5f00d914b2437732d2990d53c6c7dc0dd90 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', target_registration_id: 'str') -> 'TargetCallbackAccess'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "issue_access",
  "owner": "stove0_core.TargetCallbackAuthority",
  "unit": "member"
}
```

</details>
