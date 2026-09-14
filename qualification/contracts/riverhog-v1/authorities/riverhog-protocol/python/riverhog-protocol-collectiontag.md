# riverhog_protocol.CollectionTag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontag:c506bc696e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2999906b32"></a>
- <a id="s-bb438404c3"></a>`distribution`: `riverhog-protocol`
- <a id="s-1e57f8dada"></a>`module`: `riverhog_protocol`
- <a id="s-4379ae9505"></a>`name`: `CollectionTag`
- <a id="s-3bc26797b2"></a>`unit`: `export`

### Declared structure

- <a id="s-625f002ee8"></a>`kind`: `"type-alias"`
- <a id="s-aa8ebd7f2d"></a>`value`: `"typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=True, min_length=1, max_length=65536, pattern=None, ascii_only=None), AfterValidator(func=<function validate_collection_tag>), FieldInfo(annotation=NoneType, required=True, json_schema_extra={'x-riverhog-encoded-bytes-max': 65536, 'x-riverhog-extent': {'policy': 'contract_max', 'reason': 'bounded-human-authored-collection-tag'}, 'x-unicode-normalization': 'NFC'})]"`

## Governing policies

- <a id="pa-19c055e6a2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTag`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c2be69e67551ba758f2d7ee6f3bb2a9b36559062060f1721000686c00096351 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=True, min_length=1, max_length=65536, pattern=None, ascii_only=None), AfterValidator(func=<function validate_collection_tag>), FieldInfo(annotation=NoneType, required=True, json_schema_extra={'x-riverhog-encoded-bytes-max': 65536, 'x-riverhog-extent': {'policy': 'contract_max', 'reason': 'bounded-human-authored-collection-tag'}, 'x-unicode-normalization': 'NFC'})]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionTag",
  "unit": "export"
}
```
