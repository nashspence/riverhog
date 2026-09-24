# a_stove0_media_metadata_contract_lib.MEDIA_METADATA_OBSERVER_CONTRACT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-medi-fe97f7aa17:6af7a6d7a5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-963c084255"></a>
- <a id="s-65f28bed6d"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-e37501737d"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-187373d60d"></a>`name`: `MEDIA_METADATA_OBSERVER_CONTRACT`
- <a id="s-9656d44c4c"></a>`unit`: `export`

### Declared structure

- <a id="s-93bae0c4db"></a>`kind`: `"object"`
- <a id="s-9797d22292"></a>`type`: `"stove0_protocol.models.ObserverContract"`

## Governing policies

- <a id="pa-cf957c0105"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.MEDIA_METADATA_OBSERVER_CONTRACT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6197345b58931a930b25ed9476202b837c57ed12d7c8f2e69d9a192807f91fd9 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_protocol.models.ObserverContract"
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "MEDIA_METADATA_OBSERVER_CONTRACT",
  "unit": "export"
}
```

</details>
