# riverhog_client.processing.IncrementalDerivedCollectionWriter.finish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-incrementalder-ad83a92bb1:bcbafea4ea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fd2cd9a19d"></a>
- <a id="s-9c0d37326f"></a>`distribution`: `riverhog-client`
- <a id="s-49d17997d1"></a>`module`: `riverhog_client.processing`
- <a id="s-51647736f0"></a>`name`: `finish`
- <a id="s-27a47968ae"></a>`owner`: `riverhog_client.processing.IncrementalDerivedCollectionWriter`
- <a id="s-a2858eead2"></a>`unit`: `member`

### Declared structure

- <a id="s-27e32b5c14"></a>`kind`: `"method"`
- <a id="s-3d337f2a48"></a>`signature`: `"\"(self, *, execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'DerivedCollectionReceipt'\""`

## Maintained corroboration

### Related interface records

- [IncrementalDerivedCollectionWriter](riverhog-client-processing-incrementalderivedcollectionwriter.md)

## Governing policies

- <a id="pa-d95487e3da"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.IncrementalDerivedCollectionWriter.finish`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 097e0e08740eeae90cb07b423c2d40f750cc34b7ddb5a3a25082e3e8d0c19898 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'DerivedCollectionReceipt'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "finish",
  "owner": "riverhog_client.processing.IncrementalDerivedCollectionWriter",
  "unit": "member"
}
```

</details>
