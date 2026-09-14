# riverhog_storage_adapter_s3_support.S3ReadPreparation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3rea-d7c1220124:3407e4b662 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-78bfa33fee"></a>
- <a id="s-fe614b4d44"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-cfa235288d"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-0ebd767e5d"></a>`name`: `S3ReadPreparation`
- <a id="s-edb334e96f"></a>`unit`: `export`

### Declared structure

- <a id="s-41ab6700d3"></a>`kind`: `"class"`
- <a id="s-a754305a6e"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [prepare](riverhog-storage-adapter-s3-support-s3readpreparation-prepare.md)
- [status](riverhog-storage-adapter-s3-support-s3readpreparation-status.md)
- [cleanup](riverhog-storage-adapter-s3-support-s3readpreparation-cleanup.md)

## Governing policies

- <a id="pa-e8b4cea588"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — `reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3ReadPreparation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 006e3b29be9d696608cc4a8ecbc72d4af0876ec6c88f5f3e9e2c319fea7e086c -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "S3ReadPreparation",
  "unit": "export"
}
```
