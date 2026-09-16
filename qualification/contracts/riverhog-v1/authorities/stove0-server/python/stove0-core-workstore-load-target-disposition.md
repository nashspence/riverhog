# stove0_core.WorkStore.load_target_disposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-load-target-disposition:bd7f7a9e2b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e615cc3c90"></a>
- <a id="s-26e50a8c20"></a>`distribution`: `stove0-server`
- <a id="s-efab099863"></a>`module`: `stove0_core`
- <a id="s-34ad9a2eb8"></a>`name`: `load_target_disposition`
- <a id="s-ce9196a510"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-60bebef44a"></a>`unit`: `member`

### Declared structure

- <a id="s-233aea9131"></a>`kind`: `"method"`
- <a id="s-19e18b706f"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', input_id: 'str') -> 'InputDispositionDeclaration \| None'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-1de7f9ca00"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.load_target_disposition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 460414cc7f5862f29278c0e6e0de1f2f36e3f83cdb0ecb70ca6b008bdaf27e2c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', input_id: 'str') -> 'InputDispositionDeclaration | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_disposition",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
