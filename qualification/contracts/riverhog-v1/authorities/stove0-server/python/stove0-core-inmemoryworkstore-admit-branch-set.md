# stove0_core.InMemoryWorkStore.admit_branch_set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-admit-branch-set:65f4301d5a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cf099b1dbd"></a>
- <a id="s-fd15a06b9b"></a>`distribution`: `stove0-server`
- <a id="s-a89f3ecab5"></a>`module`: `stove0_core`
- <a id="s-90a285e08e"></a>`name`: `admit_branch_set`
- <a id="s-44758f8791"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-6b16d1d609"></a>`unit`: `member`

### Declared structure

- <a id="s-18bfff1fb1"></a>`kind`: `"method"`
- <a id="s-b6bfae5774"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int', decision: 'BranchSetDecision') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-3e3a0ce40d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.admit_branch_set`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 384640f9a175144d2cc30754dce90afc9b0ff86e0a87ee881070368e2e6e1ec0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', decision: 'BranchSetDecision') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "admit_branch_set",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
