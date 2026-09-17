# riverhog_provenance.ProvenanceVolumeDocument.to_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancevolumedocum-fc27286817:60d534aea9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5fd28975b7"></a>
- <a id="s-00a3b9b201"></a>`distribution`: `riverhog-provenance`
- <a id="s-47f0f81c58"></a>`module`: `riverhog_provenance`
- <a id="s-2c64431b25"></a>`name`: `to_mapping`
- <a id="s-7f3f2640a3"></a>`owner`: `riverhog_provenance.ProvenanceVolumeDocument`
- <a id="s-0d951e0b41"></a>`unit`: `member`

### Declared structure

- <a id="s-f31be76b68"></a>`kind`: `"method"`
- <a id="s-e2e446b2af"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [ProvenanceVolumeDocument](riverhog-provenance-provenancevolumedocument.md)

## Governing policies

- <a id="pa-0d305962af"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceVolumeDocument.to_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d1d83582d800f95f8058ba46a137e52250ebf3de12c236c0979dcb2d7de6520 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "to_mapping",
  "owner": "riverhog_provenance.ProvenanceVolumeDocument",
  "unit": "member"
}
```

</details>
