# stove0_opus_target.OpusTargetService.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-target:stove0-opus-target-opustargetservice-close:14316af78b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fa674d2cbd"></a>
- <a id="s-9e8889e774"></a>`distribution`: `stove0-opus-target`
- <a id="s-7482496d61"></a>`module`: `stove0_opus_target`
- <a id="s-090fe0b97d"></a>`name`: `close`
- <a id="s-265f912952"></a>`owner`: `stove0_opus_target.OpusTargetService`
- <a id="s-8a448c0d4f"></a>`unit`: `member`

### Declared structure

- <a id="s-decafe317e"></a>`kind`: `"method"`
- <a id="s-4d2885839a"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-814b7d2bf5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-opus-target:stove0_opus_target](../../../evidence/sources/authorities.md#src-9164f15983) — [reference/stove0/targets/opus/target/src/stove0\_opus\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/opus/target/src/stove0_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_opus_target.OpusTargetService.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca502be442bed37a5ba04077f120a7817200c36a7ec96184dbe5cc55dedc904d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "stove0-opus-target",
  "module": "stove0_opus_target",
  "name": "close",
  "owner": "stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```

</details>
