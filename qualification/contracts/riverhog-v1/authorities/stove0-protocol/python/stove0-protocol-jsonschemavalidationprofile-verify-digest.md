# stove0_protocol.JsonSchemaValidationProfile.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-jsonschemavalidationprofi-3730767c3c:8005996a15 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2d96cec7d4"></a>
- <a id="s-9647fc6832"></a>`distribution`: `stove0-protocol`
- <a id="s-89f1837ab7"></a>`module`: `stove0_protocol`
- <a id="s-ab60e6efe4"></a>`name`: `verify_digest`
- <a id="s-f60eb4bd85"></a>`owner`: `stove0_protocol.JsonSchemaValidationProfile`
- <a id="s-4b2966c428"></a>`unit`: `member`

### Declared structure

- <a id="s-086b0b915b"></a>`kind`: `"method"`
- <a id="s-857d2e736f"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [JsonSchemaValidationProfile](stove0-protocol-jsonschemavalidationprofile.md)

## Governing policies

- <a id="pa-cd04560bb7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JsonSchemaValidationProfile.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9eaf95c2991c1453c08c4c5d04819e2121f5d51529cdf04c701ab0cadfaeeea1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_digest",
  "owner": "stove0_protocol.JsonSchemaValidationProfile",
  "unit": "member"
}
```

</details>
