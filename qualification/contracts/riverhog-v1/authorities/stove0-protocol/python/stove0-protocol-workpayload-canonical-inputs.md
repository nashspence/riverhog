# stove0_protocol.WorkPayload.canonical_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workpayload-canonical-inputs:71dba2d463 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-10924e22f2"></a>
- <a id="s-21ab6539e6"></a>`distribution`: `stove0-protocol`
- <a id="s-936ce96d21"></a>`module`: `stove0_protocol`
- <a id="s-d990d42932"></a>`name`: `canonical_inputs`
- <a id="s-5f40e5989f"></a>`owner`: `stove0_protocol.WorkPayload`
- <a id="s-b82438776c"></a>`unit`: `member`

### Declared structure

- <a id="s-0bc200078f"></a>`kind`: `"classmethod"`
- <a id="s-6aeba79886"></a>`signature`: `"\"(cls, value: 'tuple[CollectionRootRef, ...]') -> 'tuple[CollectionRootRef, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkPayload](stove0-protocol-workpayload.md)

## Governing policies

- <a id="pa-6530582fa6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkPayload.canonical_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 749c8665470fc8330c6c426cfeb12eeae646a283431702cce4f20f2b003df4cf -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[CollectionRootRef, ...]') -> 'tuple[CollectionRootRef, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_inputs",
  "owner": "stove0_protocol.WorkPayload",
  "unit": "member"
}
```

</details>
