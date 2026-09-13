# riverhog_client.transform.IncrementalDerivedCollectionWriter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-incrementalderi-85c223dd31:3924496572 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7db501b083"></a>
| Field | Shape |
|---|---|
| <a id="s-d1a4aeea24"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-2c83e2732d"></a>`distribution` | "riverhog-client" |
| <a id="s-44f3b551de"></a>`module` | "riverhog_client.transform" |
| <a id="s-eb508ef3b1"></a>`name` | "IncrementalDerivedCollectionWriter" |
| <a id="s-597d69f6bd"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.IncrementalDerivedCollectionWriter.heartbeat](riverhog-client-transform-incrementalderivedcollectionwriter-heartbeat.md)
- [riverhog_client.transform.IncrementalDerivedCollectionWriter.finish](riverhog-client-transform-incrementalderivedcollectionwriter-finish.md)
- [riverhog_client.transform.IncrementalDerivedCollectionWriter.append](riverhog-client-transform-incrementalderivedcollectionwriter-append.md)
- [riverhog_client.transform.IncrementalDerivedCollectionWriter.stop](riverhog-client-transform-incrementalderivedcollectionwriter-stop.md)

## Governing policies

- <a id="pa-215a3e81b5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.IncrementalDerivedCollectionWriter`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7de77625f5431b7a9c0c3714089adc667a310868cb2ec487752441fd8e9e4650 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'Any', *, spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', producer_app: 'str', producer_version: 'str', execution_envelope_sha256: 'str', source_context: 'Mapping[str, object] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "IncrementalDerivedCollectionWriter",
  "unit": "export"
}
```
