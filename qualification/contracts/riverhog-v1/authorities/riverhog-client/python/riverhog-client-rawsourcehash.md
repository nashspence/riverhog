# riverhog_client.RawSourceHash

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-rawsourcehash:bed81b80de -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-234916118d"></a>
- <a id="s-5f3a5c3612"></a>`distribution`: `riverhog-client`
- <a id="s-71420844ff"></a>`module`: `riverhog_client`
- <a id="s-fea8021875"></a>`name`: `RawSourceHash`
- <a id="s-d473814d63"></a>`unit`: `export`

### Declared structure

- <a id="s-03aaae173b"></a>`kind`: `"class"`
- <a id="s-400b765554"></a>`signature`: `"\"(summary: 'RawSourceDigestSummary', _parts: 'BinaryIO') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-6401482012"></a>`summary` | `'RawSourceDigestSummary'` | `required` |
| <a id="s-0d2167a1dc"></a>`_parts` | `'BinaryIO'` | `required` |

## Maintained corroboration

### Related interface records

- [close](riverhog-client-rawsourcehash-close.md)
- [iter_batches](riverhog-client-rawsourcehash-iter-batches.md)

## Governing policies

- <a id="pa-09de909309"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.RawSourceHash`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44d4819ff384698db2250c677bfe028732dbb53bfb38f5f547dedbe7403aa9a4 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "summary",
        "type": "'RawSourceDigestSummary'"
      },
      {
        "default": "required",
        "name": "_parts",
        "type": "'BinaryIO'"
      }
    ],
    "kind": "class",
    "signature": "\"(summary: 'RawSourceDigestSummary', _parts: 'BinaryIO') -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "RawSourceHash",
  "unit": "export"
}
```

</details>
