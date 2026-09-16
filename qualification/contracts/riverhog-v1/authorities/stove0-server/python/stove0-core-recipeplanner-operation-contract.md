# stove0_core.RecipePlanner.operation_contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipeplanner-operation-contract:6426795b36 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8cd29fbdab"></a>
- <a id="s-4202c3b4bf"></a>`distribution`: `stove0-server`
- <a id="s-94496bac98"></a>`module`: `stove0_core`
- <a id="s-9ed291e086"></a>`name`: `operation_contract`
- <a id="s-47b4090bb8"></a>`owner`: `stove0_core.RecipePlanner`
- <a id="s-9edf569710"></a>`unit`: `member`

### Declared structure

- <a id="s-d35dd3ee81"></a>`kind`: `"method"`
- <a id="s-7fd04b2507"></a>`signature`: `"\"(self, operation: 'OperationRef') -> 'OperationContract'\""`

## Maintained corroboration

### Related interface records

- [RecipePlanner](stove0-core-recipeplanner.md)

## Governing policies

- <a id="pa-d236d70e9c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipePlanner.operation_contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae0106e035ea43830b6cbefef479564a914422d18b0cd516486ac23575191c17 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, operation: 'OperationRef') -> 'OperationContract'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "operation_contract",
  "owner": "stove0_core.RecipePlanner",
  "unit": "member"
}
```

</details>
