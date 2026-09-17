# stove0_core.WorkStore.load_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-load-selection:53f7055526 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f8e823a434"></a>
- <a id="s-4540ca34ff"></a>`distribution`: `stove0-server`
- <a id="s-387ecfb150"></a>`module`: `stove0_core`
- <a id="s-24f125e2b3"></a>`name`: `load_selection`
- <a id="s-b2ffffc2e1"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-49cd45e722"></a>`unit`: `member`

### Declared structure

- <a id="s-d6d6a4dd6d"></a>`kind`: `"method"`
- <a id="s-c48fe14367"></a>`signature`: `"\"(self, selection_sha256: 'str') -> 'ArtifactSelection \| None'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-0447f29727"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.load_selection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a65e0c82d917acff8c167e28c30c7492474e9fa6e314e5d120cfea484ea61c70 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelection | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_selection",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
