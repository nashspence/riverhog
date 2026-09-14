# riverhog_archive_contracts.SegmentFilePlacement.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-segmentfilepla-d765316c77:d00cb987cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d85cedace5"></a>
- <a id="s-d187250105"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-c059da5668"></a>`module`: `riverhog_archive_contracts`
- <a id="s-3407d395cc"></a>`name`: `from_mapping`
- <a id="s-d1a95225dd"></a>`owner`: `riverhog_archive_contracts.SegmentFilePlacement`
- <a id="s-59fe063fe8"></a>`unit`: `member`

### Declared structure

- <a id="s-6671d12def"></a>`kind`: `"classmethod"`
- <a id="s-f74ec2d885"></a>`signature`: `"\"(cls, value: 'object', *, plaintext_bytes: 'int') -> 'SegmentFilePlacement'\""`

## Maintained corroboration

### Related interface records

- [SegmentFilePlacement](riverhog-archive-contracts-segmentfileplacement.md)

## Governing policies

- <a id="pa-3d814c8195"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.SegmentFilePlacement.from_mapping`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e68f2a15210f6fdf6218ad84bd917bc560637e8e19dbde155bcc250a32d144d -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'object', *, plaintext_bytes: 'int') -> 'SegmentFilePlacement'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "from_mapping",
  "owner": "riverhog_archive_contracts.SegmentFilePlacement",
  "unit": "member"
}
```
