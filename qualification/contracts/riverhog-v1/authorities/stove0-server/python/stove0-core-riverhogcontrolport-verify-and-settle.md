# stove0_core.RiverhogControlPort.verify_and_settle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-verify-and-settle:19b4dc06bf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-52ea7ab79e"></a>
- <a id="s-15cb3026de"></a>`distribution`: `stove0-server`
- <a id="s-d3b9bdfb14"></a>`module`: `stove0_core`
- <a id="s-ea24596723"></a>`name`: `verify_and_settle`
- <a id="s-b94389ea30"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-3778a03c87"></a>`unit`: `member`

### Declared structure

- <a id="s-7ca95e3bcd"></a>`kind`: `"method"`
- <a id="s-7c860a1fc0"></a>`signature`: `"\"(self, record: 'WorkRecord', parent_outcome: 'ParentOutcomeBinding \| None' = None) -> 'tuple[OutputCollectionRef, TargetSettlementAuthority \| None]'\""`

## Maintained corroboration

### Related interface records

- [RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-f41091851a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.verify_and_settle`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c769f4f5830d155d26962eb275a9b0e0a84ac18341555dc6c44e5c469c8cc51b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', parent_outcome: 'ParentOutcomeBinding | None' = None) -> 'tuple[OutputCollectionRef, TargetSettlementAuthority | None]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "verify_and_settle",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```

</details>
