# stove0_core.WorkStore.retain_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-retain-selection:987fb44d20 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e2190de823"></a>
- <a id="s-ec60969e1d"></a>`distribution`: `stove0-server`
- <a id="s-36688e4855"></a>`module`: `stove0_core`
- <a id="s-4f91dc1144"></a>`name`: `retain_selection`
- <a id="s-89e9f6b7a4"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-5e7ae2ce7c"></a>`unit`: `member`

### Declared structure

- <a id="s-5063403087"></a>`kind`: `"method"`
- <a id="s-be0b5b791b"></a>`signature`: `"\"(self, selection: 'ArtifactSelection') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-ef6d847ba1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.retain_selection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a51de6dd27175d9b8199de7fbca3d1903bf94951c3fee7adf9141b968a0cf3b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection: 'ArtifactSelection') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retain_selection",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
