# stove0_core.HttpDepartureTargetPort.has_registration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httpdeparturetargetport-has-registration:69652c5f59 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-62556e85fe"></a>
- <a id="s-031abf34f7"></a>`distribution`: `stove0-server`
- <a id="s-254fa7d073"></a>`module`: `stove0_core`
- <a id="s-131f9c7d95"></a>`name`: `has_registration`
- <a id="s-6468a494ad"></a>`owner`: `stove0_core.HttpDepartureTargetPort`
- <a id="s-51f3875fa0"></a>`unit`: `member`

### Declared structure

- <a id="s-6cfe4e9050"></a>`kind`: `"method"`
- <a id="s-8fa87b949a"></a>`signature`: `"\"(self, registration_id: 'str') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [HttpDepartureTargetPort](stove0-core-httpdeparturetargetport.md)

## Governing policies

- <a id="pa-e14f3604e7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.HttpDepartureTargetPort.has_registration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 165f5cdf7b669cfa557cd1713a91f84c806a4e6945f1ac2b4f230eda7a3724f7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "has_registration",
  "owner": "stove0_core.HttpDepartureTargetPort",
  "unit": "member"
}
```

</details>
