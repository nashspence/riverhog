# stove0_protocol.ExecutionEnvelope.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-executionenvelope-verify-digest:3282c363c0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1a83cee2cd"></a>
- <a id="s-991695e2ae"></a>`distribution`: `stove0-protocol`
- <a id="s-6603f3b2c5"></a>`module`: `stove0_protocol`
- <a id="s-a321445a89"></a>`name`: `verify_digest`
- <a id="s-c986933e47"></a>`owner`: `stove0_protocol.ExecutionEnvelope`
- <a id="s-24ff83a461"></a>`unit`: `member`

### Declared structure

- <a id="s-8e81792918"></a>`kind`: `"method"`
- <a id="s-546aa317e8"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ExecutionEnvelope](stove0-protocol-executionenvelope.md)

## Governing policies

- <a id="pa-42b3dd9172"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ExecutionEnvelope.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a6457eb2de53c00e33bfa5f006fa7b0727a3afaa9ced02974065e390cea581e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_digest",
  "owner": "stove0_protocol.ExecutionEnvelope",
  "unit": "member"
}
```

</details>
