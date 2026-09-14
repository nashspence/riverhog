# stove0_core.RiverhogApi.create_or_resume_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-create-or-resume-44251099ee:70e4beb169 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-729d896749"></a>
- <a id="s-366a863f7b"></a>`distribution`: `stove0-server`
- <a id="s-2a631a0638"></a>`module`: `stove0_core`
- <a id="s-7f6e1d743b"></a>`name`: `create_or_resume_processing_claim`
- <a id="s-5ab34603c3"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-55bb62d9c0"></a>`unit`: `member`

### Declared structure

- <a id="s-3c5fea89ea"></a>`kind`: `"method"`
- <a id="s-e41cd0cc1d"></a>`signature`: `"\"(self, *, work_id: 'str', work_document: 'Mapping[str, Any]', work_document_sha256: 'str', inputs: 'Iterable[Mapping[str, Any]]', lease_seconds: 'int' = 1800, purpose: 'str' = 'collection-work/v1') -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-5b2628d722"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.create_or_resume_processing_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35569a7c66c04fa4e82868278e0da0adf35a0510e14466ba201bd2d706ad4d2e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, work_id: 'str', work_document: 'Mapping[str, Any]', work_document_sha256: 'str', inputs: 'Iterable[Mapping[str, Any]]', lease_seconds: 'int' = 1800, purpose: 'str' = 'collection-work/v1') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create_or_resume_processing_claim",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```
