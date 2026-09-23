# stove0_observer_protocol.canonical_json_sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-canonical-json-sha256:119d5207ad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf9165aa0e"></a>
- <a id="s-0f197a7d7e"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-fcef288af5"></a>`module`: `stove0_observer_protocol`
- <a id="s-3373280f50"></a>`name`: `canonical_json_sha256`
- <a id="s-1b18bc4f15"></a>`unit`: `export`

### Declared structure

- <a id="s-f064b64b18"></a>`kind`: `"function"`
- <a id="s-f6f186c8e5"></a>`signature`: `"\"(value: 'object') -> 'str'\""`

## Governing policies

- <a id="pa-568770d001"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.canonical_json_sha256`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29cce00e0c7409a47d267c8e785c3f38e829e6870ad62a208927ed5a60fca5c3 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'str'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_json_sha256",
  "unit": "export"
}
```

</details>
