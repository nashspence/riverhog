# riverhog_provenance.ProvenanceRootDocument.identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancerootdocument-identity:0c8881e5e9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dc384c38ad"></a>
- <a id="s-0b57ce097b"></a>`distribution`: `riverhog-provenance`
- <a id="s-6dcd5d2aa2"></a>`module`: `riverhog_provenance`
- <a id="s-b6dffcca27"></a>`name`: `identity`
- <a id="s-67dbac32ee"></a>`owner`: `riverhog_provenance.ProvenanceRootDocument`
- <a id="s-344e22582d"></a>`unit`: `member`

### Declared structure

- <a id="s-69301e7249"></a>`kind`: `"property"`
- <a id="s-98ceabf05c"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [ProvenanceRootDocument](riverhog-provenance-provenancerootdocument.md)

## Governing policies

- <a id="pa-e9e44c117a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceRootDocument.identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 271baf3627fc77f8aa88fcc8a1ec2c2ee0474eb0b2658f2c550cfa8f7670d9ab -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "identity",
  "owner": "riverhog_provenance.ProvenanceRootDocument",
  "unit": "member"
}
```

</details>
