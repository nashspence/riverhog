# stove0_api_client.Stove0ApiClient.health_ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-health-ready:4128185d03 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4d3eaf9bfc"></a>
- <a id="s-209747f784"></a>`distribution`: `stove0-api-client`
- <a id="s-fd3d5251b5"></a>`module`: `stove0_api_client`
- <a id="s-228e7c16a1"></a>`name`: `health_ready`
- <a id="s-02cefcf76a"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-5538048941"></a>`unit`: `member`

### Declared structure

- <a id="s-ec1f592ee7"></a>`kind`: `"method"`
- <a id="s-754fccf94e"></a>`signature`: `"\"(self) -> 'HealthResponse'\""`

## Maintained corroboration

### Related interface records

- [stove0 health](../../stove0-client/cli/stove0-health.md)
- [GET /health/ready](../../stove0/http-operations/get-health-ready.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-0f8e1712fc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`
- **Client method:** [reference/stove0/packages/api-client/src/stove0_api_client/client.py::Stove0ApiClient.health_ready](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L136)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.health_ready`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d97a5bd41fba0ef4398f808f35fc24a80f29fcbd6ec3ca3721ab804a0ad5c61 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'HealthResponse'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "health_ready",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
