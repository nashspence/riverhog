# stove0_core.PreviewAcceptance.from_preview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-previewacceptance-from-preview:60f365747d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9af73c2e93"></a>
- <a id="s-437f3595ef"></a>`distribution`: `stove0-server`
- <a id="s-055f2423a7"></a>`module`: `stove0_core`
- <a id="s-9e29d4492f"></a>`name`: `from_preview`
- <a id="s-50c4b708e2"></a>`owner`: `stove0_core.PreviewAcceptance`
- <a id="s-e44490b2c9"></a>`unit`: `member`

### Declared structure

- <a id="s-db0334709d"></a>`kind`: `"classmethod"`
- <a id="s-7eb10460db"></a>`signature`: `"\"(cls, preview: 'WorkflowPreview') -> 'PreviewAcceptance'\""`

## Maintained corroboration

### Related interface records

- [PreviewAcceptance](stove0-core-previewacceptance.md)

## Governing policies

- <a id="pa-0e2f3848f8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.PreviewAcceptance.from_preview`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b8ea7229f41eaf09fa021b20ccecbbd71f80c2f0b3718fe7e965c1de1641393 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, preview: 'WorkflowPreview') -> 'PreviewAcceptance'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "from_preview",
  "owner": "stove0_core.PreviewAcceptance",
  "unit": "member"
}
```

</details>
