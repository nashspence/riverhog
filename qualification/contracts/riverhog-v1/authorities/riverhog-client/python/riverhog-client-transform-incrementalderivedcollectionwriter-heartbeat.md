# riverhog_client.transform.IncrementalDerivedCollectionWriter.heartbeat

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-incrementalderi-3d35767ad7:fd8205ddc8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-22a9e8ea98"></a>
- <a id="s-da1d92cd4f"></a>`distribution`: `riverhog-client`
- <a id="s-dd9aed81ef"></a>`module`: `riverhog_client.transform`
- <a id="s-83a554b0c1"></a>`name`: `heartbeat`
- <a id="s-ba13d89d89"></a>`owner`: `riverhog_client.transform.IncrementalDerivedCollectionWriter`
- <a id="s-5b19591bdb"></a>`unit`: `member`

### Declared structure

- <a id="s-356aaeb249"></a>`kind`: `"method"`
- <a id="s-3673c6e9fe"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [IncrementalDerivedCollectionWriter](riverhog-client-transform-incrementalderivedcollectionwriter.md)

## Governing policies

- <a id="pa-e5126b2c09"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.IncrementalDerivedCollectionWriter.heartbeat`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12b9d4e2c74dac1ead984e3c0eccb41eb40a2c113b8dc25827fd139fca9ed023 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "heartbeat",
  "owner": "riverhog_client.transform.IncrementalDerivedCollectionWriter",
  "unit": "member"
}
```

</details>
