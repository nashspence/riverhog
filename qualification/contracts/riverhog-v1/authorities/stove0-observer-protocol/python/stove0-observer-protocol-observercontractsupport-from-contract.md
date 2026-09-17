# stove0_observer_protocol.ObserverContractSupport.from_contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontract-abca90eb9c:b6eb3c9e12 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-572a623611"></a>
- <a id="s-63320b1518"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-fadb5d0945"></a>`module`: `stove0_observer_protocol`
- <a id="s-958d80f836"></a>`name`: `from_contract`
- <a id="s-f1fb240701"></a>`owner`: `stove0_observer_protocol.ObserverContractSupport`
- <a id="s-f5116db747"></a>`unit`: `member`

### Declared structure

- <a id="s-46216b8a9b"></a>`kind`: `"classmethod"`
- <a id="s-4f1bf750fb"></a>`signature`: `"\"(cls, value: 'ObserverContract', *, preferred_subject_batch_size: 'int' = 128) -> 'ObserverContractSupport'\""`

## Maintained corroboration

### Related interface records

- [ObserverContractSupport](stove0-observer-protocol-observercontractsupport.md)

## Governing policies

- <a id="pa-30ac5e647b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContractSupport.from_contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 70b5d87d199efb082c567875332c4dcf42e86a740a052b7de067ad30d5d9ecc5 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'ObserverContract', *, preferred_subject_batch_size: 'int' = 128) -> 'ObserverContractSupport'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "from_contract",
  "owner": "stove0_observer_protocol.ObserverContractSupport",
  "unit": "member"
}
```

</details>
