# stove0_target_protocol.SemanticValidationProfile.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-semanticvalidation-a66e9fec44:91168855bc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b5bd271fc"></a>
- <a id="s-69d39de20d"></a>`distribution`: `stove0-target-protocol`
- <a id="s-689f6ec89e"></a>`module`: `stove0_target_protocol`
- <a id="s-02ae9cb7d2"></a>`name`: `verify_digest`
- <a id="s-e403d39530"></a>`owner`: `stove0_target_protocol.SemanticValidationProfile`
- <a id="s-c1914be543"></a>`unit`: `member`

### Declared structure

- <a id="s-612f28f3b3"></a>`kind`: `"method"`
- <a id="s-ad0dbfacb4"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SemanticValidationProfile](stove0-target-protocol-semanticvalidationprofile.md)

## Governing policies

- <a id="pa-a2098183b9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.SemanticValidationProfile.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 089a247a53c940a78c8cd649481c58b9a7e1c4a0b15b02dd87e2da7274799c69 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "verify_digest",
  "owner": "stove0_target_protocol.SemanticValidationProfile",
  "unit": "member"
}
```

</details>
