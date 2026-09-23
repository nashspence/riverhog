# stove0_target_protocol.TargetInputPage.bind_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetinputpage-bind-page:4f8cef3dc8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-415d49bcc2"></a>
- <a id="s-4479ea6897"></a>`distribution`: `stove0-target-protocol`
- <a id="s-f26347e674"></a>`module`: `stove0_target_protocol`
- <a id="s-a398d2c025"></a>`name`: `bind_page`
- <a id="s-f50702366a"></a>`owner`: `stove0_target_protocol.TargetInputPage`
- <a id="s-7b073f2a16"></a>`unit`: `member`

### Declared structure

- <a id="s-6e35e806ea"></a>`kind`: `"method"`
- <a id="s-e366fc8283"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetInputPage](stove0-target-protocol-targetinputpage.md)

## Governing policies

- <a id="pa-bf0b9dee3c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetInputPage.bind_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0b81b71039a3017bd301db408dd77369d9270b63f44cea7143d496fd5104580 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "bind_page",
  "owner": "stove0_target_protocol.TargetInputPage",
  "unit": "member"
}
```

</details>
