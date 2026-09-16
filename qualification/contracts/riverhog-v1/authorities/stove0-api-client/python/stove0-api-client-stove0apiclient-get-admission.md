# stove0_api_client.Stove0ApiClient.get_admission

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-get-admission:d1bb315339 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b70eb65989"></a>
- <a id="s-115df161cc"></a>`distribution`: `stove0-api-client`
- <a id="s-6ef6542faa"></a>`module`: `stove0_api_client`
- <a id="s-095be7d006"></a>`name`: `get_admission`
- <a id="s-3b2aab503e"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-668272ff6d"></a>`unit`: `member`

### Declared structure

- <a id="s-d77253988b"></a>`kind`: `"method"`
- <a id="s-e87b456dc1"></a>`signature`: `"\"(self, admission_id: 'str') -> 'AdmissionView'\""`

## Maintained corroboration

### Related interface records

- [stove0 admission show](../../stove0-client/cli/stove0-admission-show.md)
- [GET /v1/admissions/{admission_id}](../../stove0/http-operations/get-v1-admissions-admission-id.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-6232c2400e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`
- **Client method:** [reference/stove0/packages/api-client/src/stove0_api_client/client.py::Stove0ApiClient.get_admission](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L214)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.get_admission`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fcba24ba92b3d98087c608c61156f9c47a64cd1f8c131f4d5c345adecd3ee83f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, admission_id: 'str') -> 'AdmissionView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "get_admission",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
