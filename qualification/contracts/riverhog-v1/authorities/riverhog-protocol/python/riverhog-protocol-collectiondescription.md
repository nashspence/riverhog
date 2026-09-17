# riverhog_protocol.CollectionDescription

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiondescription:343c99e66e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a6761f550a"></a>
- <a id="s-9676cde39c"></a>`distribution`: `riverhog-protocol`
- <a id="s-49a465e29a"></a>`module`: `riverhog_protocol`
- <a id="s-0f67d945e3"></a>`name`: `CollectionDescription`
- <a id="s-299e799d8f"></a>`unit`: `export`

### Declared structure

- <a id="s-7577d43ef9"></a>`kind`: `"type-alias"`
- <a id="s-3f6994e3d0"></a>`value`: `"typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=True, min_length=1, max_length=32768, pattern=None, ascii_only=None), AfterValidator(func=<function validate_collection_description>), FieldInfo(annotation=NoneType, required=True, json_schema_extra={'x-riverhog-encoded-bytes-max': 32768, 'x-riverhog-extent': {'policy': 'contract_max', 'reason': 'bounded-human-authored-catalog-description'}, 'x-unicode-normalization': 'NFC'})]"`

## Governing policies

- <a id="pa-8ff18a8f0e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDescription`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e3c6a83d7be06d988ac577ea3422a91063542014a9e41e3d217441d6af886a7 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=True, min_length=1, max_length=32768, pattern=None, ascii_only=None), AfterValidator(func=<function validate_collection_description>), FieldInfo(annotation=NoneType, required=True, json_schema_extra={'x-riverhog-encoded-bytes-max': 32768, 'x-riverhog-extent': {'policy': 'contract_max', 'reason': 'bounded-human-authored-catalog-description'}, 'x-unicode-normalization': 'NFC'})]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionDescription",
  "unit": "export"
}
```

</details>
