# stove0_core.Stove0WorkService.bind_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-bind-claim:525345e356 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e6e2ed5e42"></a>
- <a id="s-7657d27b88"></a>`distribution`: `stove0-server`
- <a id="s-22c3af7596"></a>`module`: `stove0_core`
- <a id="s-db6d9d4e67"></a>`name`: `bind_claim`
- <a id="s-5a2b584a28"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-fc0720c89e"></a>`unit`: `member`

### Declared structure

- <a id="s-7f8f3048e4"></a>`kind`: `"method"`
- <a id="s-eb937f1c23"></a>`signature`: `"\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-6734eecab1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.bind_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7754f615572a8da8b4fd505dd872385b6f98aea21279b6e703ac5f86adbc024e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "bind_claim",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
