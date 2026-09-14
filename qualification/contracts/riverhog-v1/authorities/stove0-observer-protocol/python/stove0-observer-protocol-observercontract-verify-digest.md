# stove0_observer_protocol.ObserverContract.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontract-8db74f213c:badfff9f28 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-089bb5acc7"></a>
- <a id="s-88e9cfbc79"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-bd1251a77b"></a>`module`: `stove0_observer_protocol`
- <a id="s-8553698546"></a>`name`: `verify_digest`
- <a id="s-7d97fbda13"></a>`owner`: `stove0_observer_protocol.ObserverContract`
- <a id="s-caa89d2ef6"></a>`unit`: `member`

### Declared structure

- <a id="s-c8f9dfb6cd"></a>`kind`: `"method"`
- <a id="s-faffe5782c"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ObserverContract](stove0-observer-protocol-observercontract.md)

## Governing policies

- <a id="pa-e8e652be6f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContract.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e4a5d4c5d008ebbd937516aabbe5983e1c9f97f178d7819ceb2b962d7c0eefdb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "verify_digest",
  "owner": "stove0_observer_protocol.ObserverContract",
  "unit": "member"
}
```
