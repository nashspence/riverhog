# riverhog_protocol.TransformIntent.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformintent-from-mapping:b6c4432d31 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-702aed12d3"></a>
- <a id="s-ab14ed20df"></a>`distribution`: `riverhog-protocol`
- <a id="s-3ecf27c598"></a>`module`: `riverhog_protocol`
- <a id="s-3cddf91fc4"></a>`name`: `from_mapping`
- <a id="s-8fb1ba00c4"></a>`owner`: `riverhog_protocol.TransformIntent`
- <a id="s-fa840fe22d"></a>`unit`: `member`

### Declared structure

- <a id="s-7914ec9d47"></a>`kind`: `"classmethod"`
- <a id="s-411745c3a5"></a>`signature`: `"\"(cls, value: 'Mapping[str, object]') -> 'TransformIntent'\""`

## Maintained corroboration

### Related interface records

- [TransformIntent](riverhog-protocol-transformintent.md)

## Governing policies

- <a id="pa-4ea662b419"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformIntent.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad829ef260143307de89e40675d3869fdc34745c70db95cd70e0ff9667b38c40 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Mapping[str, object]') -> 'TransformIntent'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "from_mapping",
  "owner": "riverhog_protocol.TransformIntent",
  "unit": "member"
}
```

</details>
