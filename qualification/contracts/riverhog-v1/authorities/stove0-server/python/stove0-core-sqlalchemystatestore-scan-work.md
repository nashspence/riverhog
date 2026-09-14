# stove0_core.SqlAlchemyStateStore.scan_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-scan-work:a8d40e8e27 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-06e8f37bc7"></a>
- <a id="s-675b1ed188"></a>`distribution`: `stove0-server`
- <a id="s-95b801ccee"></a>`module`: `stove0_core`
- <a id="s-b19f0f8ec3"></a>`name`: `scan_work`
- <a id="s-d16452aa77"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-6e7f56148e"></a>`unit`: `member`

### Declared structure

- <a id="s-53e8466472"></a>`kind`: `"method"`
- <a id="s-63b32dc913"></a>`signature`: `"\"(self, *, phases: 'Sequence[str]', after_work_id: 'str', limit: 'int') -> 'tuple[list[WorkRecord], str]'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-a9af85a45e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.scan_work`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 042217810e8321e646d33f19960c6b1cef66d96e30794647db36fcd35429d5de -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, phases: 'Sequence[str]', after_work_id: 'str', limit: 'int') -> 'tuple[list[WorkRecord], str]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "scan_work",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
