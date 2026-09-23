# stove0_observer_protocol.JsonSchemaValidationProfile.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-jsonschemavalida-cdfcad1b97:a7f6a802c8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5523e2f1a7"></a>
- <a id="s-98bc2837ac"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-594dea3875"></a>`module`: `stove0_observer_protocol`
- <a id="s-c77103c41e"></a>`name`: `verify_digest`
- <a id="s-71b0c8725b"></a>`owner`: `stove0_observer_protocol.JsonSchemaValidationProfile`
- <a id="s-1dc0b4d83f"></a>`unit`: `member`

### Declared structure

- <a id="s-199e67b888"></a>`kind`: `"method"`
- <a id="s-4b2411e731"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [JsonSchemaValidationProfile](stove0-observer-protocol-jsonschemavalidationprofile.md)

## Governing policies

- <a id="pa-cf1ba6b4e8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.JsonSchemaValidationProfile.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f3601c848a02756cb43a2fe0b8b0701d880266d52f033d9af8b899aa354379d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "verify_digest",
  "owner": "stove0_observer_protocol.JsonSchemaValidationProfile",
  "unit": "member"
}
```

</details>
