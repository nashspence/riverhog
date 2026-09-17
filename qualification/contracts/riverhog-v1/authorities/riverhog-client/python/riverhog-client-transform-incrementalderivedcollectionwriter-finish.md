# riverhog_client.transform.IncrementalDerivedCollectionWriter.finish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-incrementalderi-676807ef64:8a8e0aafb7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-65afc375cc"></a>
- <a id="s-80a2263825"></a>`distribution`: `riverhog-client`
- <a id="s-af2be44f21"></a>`module`: `riverhog_client.transform`
- <a id="s-959e0f1b2c"></a>`name`: `finish`
- <a id="s-a0279d69d5"></a>`owner`: `riverhog_client.transform.IncrementalDerivedCollectionWriter`
- <a id="s-1067b6816f"></a>`unit`: `member`

### Declared structure

- <a id="s-97258c690f"></a>`kind`: `"method"`
- <a id="s-fce43eb63c"></a>`signature`: `"\"(self, *, execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'DerivedCollectionReceipt'\""`

## Maintained corroboration

### Related interface records

- [IncrementalDerivedCollectionWriter](riverhog-client-transform-incrementalderivedcollectionwriter.md)

## Governing policies

- <a id="pa-93e2d6fce0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.IncrementalDerivedCollectionWriter.finish`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd6f41a00f1ed3cbdc0e820c8402720050a98c9cb80daf02cdd494b4c7b9bb91 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'DerivedCollectionReceipt'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "finish",
  "owner": "riverhog_client.transform.IncrementalDerivedCollectionWriter",
  "unit": "member"
}
```

</details>
