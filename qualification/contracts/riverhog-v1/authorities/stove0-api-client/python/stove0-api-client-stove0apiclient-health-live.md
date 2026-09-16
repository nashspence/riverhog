# stove0_api_client.Stove0ApiClient.health_live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-health-live:20cae2cdd6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-be86570206"></a>
- <a id="s-56a5144aff"></a>`distribution`: `stove0-api-client`
- <a id="s-69fb331af3"></a>`module`: `stove0_api_client`
- <a id="s-ffde4b2a7a"></a>`name`: `health_live`
- <a id="s-6426cc0a03"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-dfb7de039a"></a>`unit`: `member`

### Declared structure

- <a id="s-7ba2b4acc3"></a>`kind`: `"method"`
- <a id="s-f97783728a"></a>`signature`: `"\"(self) -> 'HealthResponse'\""`

## Maintained corroboration

### Related interface records

- [stove0 health](../../stove0-client/cli/stove0-health.md)
- [GET /health/live](../../stove0/http-operations/get-health-live.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-1b6f0d0460"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`
- **Client method:** [reference/stove0/packages/api-client/src/stove0_api_client/client.py::Stove0ApiClient.health_live](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L131)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.health_live`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e1e06b1c7d8b7898c96263b72b4586552fdeea0ec4eba0bedc098eefcc7b8de -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'HealthResponse'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "health_live",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
