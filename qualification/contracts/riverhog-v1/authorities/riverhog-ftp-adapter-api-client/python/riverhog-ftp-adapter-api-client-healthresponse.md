# riverhog_ftp_adapter_api_client.HealthResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-healthresponse:7116238a6f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-66acadb477"></a>
- <a id="s-1da8c0b112"></a>`distribution`: `riverhog-ftp-adapter-api-client`
- <a id="s-8e7d9a8a94"></a>`module`: `riverhog_ftp_adapter_api_client`
- <a id="s-d93e03a287"></a>`name`: `HealthResponse`
- <a id="s-0cd7f2fb27"></a>`unit`: `export`

### Declared structure

- <a id="s-25af24a126"></a>`kind`: `"class"`
- <a id="s-3beacfae47"></a>`signature`: `"\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""`

#### Validated model schema

<a id="s-c741b5c1c6"></a>

- <a id="s-023c15e755"></a>`type`: `"object"`
- <a id="s-956e5c9359"></a>`additionalProperties`: `false`
- <a id="s-a96db27993"></a>`required`: `["service","status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-942d1ada33"></a>`service` | yes | type="string"; minLength=1 |  |
| <a id="s-293b9c823c"></a>`status` | yes | type="string"; const="ok" |  |

## Governing policies

- <a id="pa-b74774511d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client](../../../evidence/sources.md#src-a83ae875ae) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter_api_client.HealthResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e5e1352adb25d21a3837bcf10b31956c08d0803c9199146cc87a9bbc0b990f7e -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "service": {
          "minLength": 1,
          "type": "string"
        },
        "status": {
          "const": "ok",
          "type": "string"
        }
      },
      "required": [
        "service",
        "status"
      ],
      "type": "object"
    },
    "signature": "\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""
  },
  "distribution": "riverhog-ftp-adapter-api-client",
  "module": "riverhog_ftp_adapter_api_client",
  "name": "HealthResponse",
  "unit": "export"
}
```

</details>
