# stove0_core.Stove0Coordinator.retry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0coordinator-retry:eccdabbf5d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c1490e7106"></a>
- <a id="s-e76f934559"></a>`distribution`: `stove0-server`
- <a id="s-dfc6d5c64e"></a>`module`: `stove0_core`
- <a id="s-61f74af028"></a>`name`: `retry`
- <a id="s-a8a71c4de6"></a>`owner`: `stove0_core.Stove0Coordinator`
- <a id="s-94c9a468a9"></a>`unit`: `member`

### Declared structure

- <a id="s-0736f351b6"></a>`kind`: `"method"`
- <a id="s-864d0e6c92"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0Coordinator](stove0-core-stove0coordinator.md)

## Governing policies

- <a id="pa-62692afdcc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0Coordinator.retry`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb6b75f78144cda3b27d9a2ea6e66fdfe8162770f0d41ee49b094a46b9366107 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retry",
  "owner": "stove0_core.Stove0Coordinator",
  "unit": "member"
}
```

</details>
