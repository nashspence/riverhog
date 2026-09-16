# stove0_core.WorkStore.scan_target_production_seals

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-scan-target-production-seals:25b5b35303 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b83a7bbf65"></a>
- <a id="s-d2f4c03b4d"></a>`distribution`: `stove0-server`
- <a id="s-eceed42e84"></a>`module`: `stove0_core`
- <a id="s-e5b41304e8"></a>`name`: `scan_target_production_seals`
- <a id="s-3a9754297d"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-ed599ca23b"></a>`unit`: `member`

### Declared structure

- <a id="s-f9d405653d"></a>`kind`: `"method"`
- <a id="s-6f282ff598"></a>`signature`: `"\"(self, *, state: 'TargetProductionSealState', limit: 'int') -> 'tuple[TargetProductionSealRecord, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-0ec04aa2b9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.scan_target_production_seals`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b0fcedfb2144436a6c9abaa3cd8b6ca8519b85057ea276497097de477fb549b2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, state: 'TargetProductionSealState', limit: 'int') -> 'tuple[TargetProductionSealRecord, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "scan_target_production_seals",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
