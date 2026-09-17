# stove0_media_metadata_observer_contracts.validate_media_metadata_facts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-ec568e5e0c:bf147bb209 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-73276e7157"></a>
- <a id="s-87f16fd521"></a>`distribution`: `stove0-media-metadata-observer-contracts`
- <a id="s-19abea69ad"></a>`module`: `stove0_media_metadata_observer_contracts`
- <a id="s-d5107b6916"></a>`name`: `validate_media_metadata_facts`
- <a id="s-cac4667acc"></a>`unit`: `export`

### Declared structure

- <a id="s-b04fc5b588"></a>`kind`: `"function"`
- <a id="s-0f6aa2f88f"></a>`signature`: `"\"(facts: 'Mapping[str, object]', subjects: 'Sequence[ArtifactSubject]') -> 'MediaMetadataFacts'\""`

## Governing policies

- <a id="pa-b9272ea9f3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — [reference/stove0/observers/contracts/media-metadata/src/stove0\_media\_metadata\_observer\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.validate_media_metadata_facts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60b7a28e1b0788745421fa08740887bc974978d4b88fa82dfdc0059bacdbd9f9 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(facts: 'Mapping[str, object]', subjects: 'Sequence[ArtifactSubject]') -> 'MediaMetadataFacts'\""
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "validate_media_metadata_facts",
  "unit": "export"
}
```

</details>
