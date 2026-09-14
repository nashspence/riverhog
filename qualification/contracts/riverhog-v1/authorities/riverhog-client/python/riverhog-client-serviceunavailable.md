# riverhog_client.ServiceUnavailable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-serviceunavailable:81f20e45fa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bbbf3de433"></a>
- <a id="s-653e1e9170"></a>`distribution`: `riverhog-client`
- <a id="s-238780efcc"></a>`module`: `riverhog_client`
- <a id="s-eb7ed22fac"></a>`name`: `ServiceUnavailable`
- <a id="s-0701832192"></a>`unit`: `export`

### Declared structure

- <a id="s-6ac0569f49"></a>`kind`: `"class"`
- <a id="s-f67ff71081"></a>`signature`: `"\"(message: 'str', *, code: 'str \| None' = None, observed_status: 'int \| None' = None, details: 'dict[str, Any] \| None' = None) -> 'None'\""`

## Governing policies

- <a id="pa-c45f40b629"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ServiceUnavailable`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6195d427eed91a7c6075fbdf5449d0af708eab6170920d2085dcd2e78deff004 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ServiceUnavailable",
  "unit": "export"
}
```
