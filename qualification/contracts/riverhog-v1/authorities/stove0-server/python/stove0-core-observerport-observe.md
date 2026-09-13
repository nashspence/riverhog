# stove0_core.ObserverPort.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-observerport-observe:21a75da030 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a1f0446625"></a>
| Field | Shape |
|---|---|
| <a id="s-b2cbdb9f31"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e6b98e0d7f"></a>`distribution` | "stove0-server" |
| <a id="s-6313069873"></a>`module` | "stove0_core" |
| <a id="s-251b9b17e6"></a>`name` | "observe" |
| <a id="s-b3a5647691"></a>`owner` | "stove0_core.ObserverPort" |
| <a id="s-a3171f1ae6"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.ObserverPort](stove0-core-observerport.md)

## Governing policies

- <a id="pa-c2f11643e4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.ObserverPort.observe`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d279be687aced27400360c1c2fd40b88ccfafdbadb607cff3d9ae5d968e1bcb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str', invocation: 'ObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'ObservationResult'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "observe",
  "owner": "stove0_core.ObserverPort",
  "unit": "member"
}
```
