# stove0_core.RiverhogControlPort.abandon_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-abandon-claim:ef6a92821c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-43f62d849a"></a>
- <a id="s-2eee65959e"></a>`distribution`: `stove0-server`
- <a id="s-6d5db382f6"></a>`module`: `stove0_core`
- <a id="s-2ce7d036f7"></a>`name`: `abandon_claim`
- <a id="s-66c2857e73"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-96ed289739"></a>`unit`: `member`

### Declared structure

- <a id="s-3eb3f44582"></a>`kind`: `"method"`
- <a id="s-3a22f07b50"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-cd6572cde5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.abandon_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3c1cc1fec961f1f40c1411c79641bc35df8ccecb7af7bae58920faf9e7e4d20 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "abandon_claim",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```
