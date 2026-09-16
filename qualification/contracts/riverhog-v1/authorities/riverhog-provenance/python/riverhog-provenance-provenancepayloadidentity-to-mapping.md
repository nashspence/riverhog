# riverhog_provenance.ProvenancePayloadIdentity.to_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancepayloadiden-62496bb1e3:fcee20f6ed -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dde680a806"></a>
- <a id="s-d0fbb8f277"></a>`distribution`: `riverhog-provenance`
- <a id="s-2e483c96f1"></a>`module`: `riverhog_provenance`
- <a id="s-4d36396466"></a>`name`: `to_mapping`
- <a id="s-0ca93aff5c"></a>`owner`: `riverhog_provenance.ProvenancePayloadIdentity`
- <a id="s-ed92bec69a"></a>`unit`: `member`

### Declared structure

- <a id="s-b31f2b70b1"></a>`kind`: `"method"`
- <a id="s-f3977d8692"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [ProvenancePayloadIdentity](riverhog-provenance-provenancepayloadidentity.md)

## Governing policies

- <a id="pa-6c8279bba9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenancePayloadIdentity.to_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14dd859a2ede58c6ac9a7b52ce988e937eca383efcfff99938544733114fac23 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "to_mapping",
  "owner": "riverhog_provenance.ProvenancePayloadIdentity",
  "unit": "member"
}
```

</details>
