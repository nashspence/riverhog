# stove0_core.DepartureEffectService.policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-departureeffectservice-policies:4a09d0eb76 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a55db85b8f"></a>
- <a id="s-15443aadb9"></a>`distribution`: `stove0-server`
- <a id="s-892c8a8e6f"></a>`module`: `stove0_core`
- <a id="s-c3a9629bb5"></a>`name`: `policies`
- <a id="s-fb1bfdf759"></a>`owner`: `stove0_core.DepartureEffectService`
- <a id="s-2971bb7cff"></a>`unit`: `member`

### Declared structure

- <a id="s-b3550ba62d"></a>`kind`: `"method"`
- <a id="s-2e81cce168"></a>`signature`: `"\"(self) -> 'DeparturePolicyCatalogView'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectService](stove0-core-departureeffectservice.md)

## Governing policies

- <a id="pa-904f7438ce"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.DepartureEffectService.policies`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f46b6ab9b95faaa7c562d01be82bea8c45dfbbdba778598561b561c6c6eb189d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'DeparturePolicyCatalogView'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "policies",
  "owner": "stove0_core.DepartureEffectService",
  "unit": "member"
}
```

</details>
