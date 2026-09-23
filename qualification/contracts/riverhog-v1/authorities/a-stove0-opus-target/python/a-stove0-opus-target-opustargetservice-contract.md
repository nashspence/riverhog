# a_stove0_opus_target.OpusTargetService.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-opus-target:a-stove0-opus-target-opustargetservice-contract:4f191ead6d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-45e850555c"></a>
- <a id="s-d7fb016038"></a>`distribution`: `a-stove0-opus-target`
- <a id="s-17c95406ff"></a>`module`: `a_stove0_opus_target`
- <a id="s-def6bce721"></a>`name`: `contract`
- <a id="s-50419b021a"></a>`owner`: `a_stove0_opus_target.OpusTargetService`
- <a id="s-fa0689c0f4"></a>`unit`: `member`

### Declared structure

- <a id="s-98940a7d61"></a>`kind`: `"method"`
- <a id="s-c19310d2ed"></a>`signature`: `"\"(self) -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](a-stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-4e70374068"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-opus-target:a_stove0_opus_target](../../../evidence/sources/authorities.md#src-43f70c74af) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_opus_target.OpusTargetService.contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5e588951376993e4a935b641f7f7ee45d6bbc15f1ccedb174a331304e7ebe31 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "a-stove0-opus-target",
  "module": "a_stove0_opus_target",
  "name": "contract",
  "owner": "a_stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```

</details>
