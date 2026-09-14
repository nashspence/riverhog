# stove0_core.ClassificationAdmissionService.policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-classificationadmissionservice-policies:18e041641b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-19c29bb7c1"></a>
- <a id="s-2700077e0e"></a>`distribution`: `stove0-server`
- <a id="s-e5ae73f7dc"></a>`module`: `stove0_core`
- <a id="s-98c0412912"></a>`name`: `policies`
- <a id="s-89be022cbe"></a>`owner`: `stove0_core.ClassificationAdmissionService`
- <a id="s-53c805e799"></a>`unit`: `member`

### Declared structure

- <a id="s-30ba939799"></a>`kind`: `"method"`
- <a id="s-85d4ada0f5"></a>`signature`: `"\"(self) -> 'AdmissionPolicyCatalogView'\""`

## Maintained corroboration

### Related interface records

- [ClassificationAdmissionService](stove0-core-classificationadmissionservice.md)

## Governing policies

- <a id="pa-effac852b0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.ClassificationAdmissionService.policies`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26fad6a1d39e6816086dede003b1877ecf08494223999c19c4f9b478a1d6dc85 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'AdmissionPolicyCatalogView'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "policies",
  "owner": "stove0_core.ClassificationAdmissionService",
  "unit": "member"
}
```
