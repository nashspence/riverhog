# a_riverhog_ftp_spool_client.HealthResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-healthresponse:5e9f635296 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-06992f81fd"></a>
- <a id="s-1ae76b84b6"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-9ee36b9301"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-289a45e36a"></a>`name`: `HealthResponse`
- <a id="s-391c7b086c"></a>`unit`: `export`

### Declared structure

- <a id="s-199ca3793c"></a>`kind`: `"class"`
- <a id="s-d5e0929fd5"></a>`signature`: `"\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""`

#### Validated model schema

<a id="s-ba16b3afd0"></a>

- <a id="s-b1d7460f8c"></a>`type`: `"object"`
- <a id="s-4cb26e54f9"></a>`additionalProperties`: `false`
- <a id="s-7aab5b8687"></a>`required`: `["service","status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-16e6709acc"></a>`service` | yes | type="string"; minLength=1 |  |
| <a id="s-c1e93b9423"></a>`status` | yes | type="string"; const="ok" |  |

## Governing policies

- <a id="pa-97ec22abef"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.HealthResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d47897747986a30dbdd3ac7cd373de892315a685e31490f0fece6e52adeef786 -->

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
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "HealthResponse",
  "unit": "export"
}
```

</details>
