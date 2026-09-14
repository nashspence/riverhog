# riverhog_protocol.ServiceUnavailable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-serviceunavailable:df46ff8a0c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9acd7c3811"></a>
- <a id="s-75b81df3c4"></a>`distribution`: `riverhog-protocol`
- <a id="s-75b3a3662d"></a>`module`: `riverhog_protocol`
- <a id="s-25a1f678df"></a>`name`: `ServiceUnavailable`
- <a id="s-34d51f4930"></a>`unit`: `export`

### Declared structure

- <a id="s-8cfb9409c1"></a>`kind`: `"class"`
- <a id="s-86617b1556"></a>`signature`: `"\"(message: 'str', *, code: 'str \| None' = None, observed_status: 'int \| None' = None, details: 'dict[str, Any] \| None' = None) -> 'None'\""`

## Governing policies

- <a id="pa-9a31d33e8f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ServiceUnavailable`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b2af9709d1a6be6c5f6c095fb9295cbeef9cdbb8852b596d638a709f09840db -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ServiceUnavailable",
  "unit": "export"
}
```
