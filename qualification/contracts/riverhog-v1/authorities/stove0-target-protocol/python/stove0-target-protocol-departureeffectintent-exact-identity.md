# stove0_target_protocol.DepartureEffectIntent.exact_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectint-5034c11b6f:9286a842d2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-933be23c65"></a>
- <a id="s-db33c25dd6"></a>`distribution`: `stove0-target-protocol`
- <a id="s-a5cd0eb3a5"></a>`module`: `stove0_target_protocol`
- <a id="s-0e0f586d67"></a>`name`: `exact_identity`
- <a id="s-91f14d2619"></a>`owner`: `stove0_target_protocol.DepartureEffectIntent`
- <a id="s-ea64415ba7"></a>`unit`: `member`

### Declared structure

- <a id="s-e368207c72"></a>`kind`: `"method"`
- <a id="s-93ef2e7385"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectIntent](stove0-target-protocol-departureeffectintent.md)

## Governing policies

- <a id="pa-427159255c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectIntent.exact_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b74e7ebb80b4f30ecc960bad6decd796562e7be9a0bca3bb4034e360219fdfa4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "exact_identity",
  "owner": "stove0_target_protocol.DepartureEffectIntent",
  "unit": "member"
}
```

</details>
