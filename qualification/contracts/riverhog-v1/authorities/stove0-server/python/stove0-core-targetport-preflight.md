# stove0_core.TargetPort.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetport-preflight:88367d1d60 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c6590801c0"></a>
- <a id="s-6e0a208722"></a>`distribution`: `stove0-server`
- <a id="s-0cadc1b5e2"></a>`module`: `stove0_core`
- <a id="s-40993a0b40"></a>`name`: `preflight`
- <a id="s-abfc44053a"></a>`owner`: `stove0_core.TargetPort`
- <a id="s-ce627aa294"></a>`unit`: `member`

### Declared structure

- <a id="s-3888ec30ee"></a>`kind`: `"method"`
- <a id="s-fe68db9651"></a>`signature`: `"\"(self, registration_id: 'str', request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [TargetPort](stove0-core-targetport.md)

## Governing policies

- <a id="pa-30bf307a20"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.TargetPort.preflight`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40b1b10a90b3b2c5b5b1e23ec6931e9a85c3ee30b64ce8b6448dc65cf6026521 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str', request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "preflight",
  "owner": "stove0_core.TargetPort",
  "unit": "member"
}
```

</details>
