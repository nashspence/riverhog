# a_riverhog_ftp_spool_client.HealthOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-healthout:dc545f163a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b0c04e2c10"></a>
- <a id="s-6c0af9bdeb"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-1f12f7c837"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-aa15d92a81"></a>`name`: `HealthOut`
- <a id="s-1871c3cc8a"></a>`unit`: `export`

### Declared structure

- <a id="s-3c24f3198f"></a>`kind`: `"class"`
- <a id="s-dfeb3ecede"></a>`signature`: `"\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""`

#### Validated model schema

<a id="s-c6a801d0cd"></a>

- <a id="s-995615ec4f"></a>`type`: `"object"`
- <a id="s-1c89900673"></a>`additionalProperties`: `false`
- <a id="s-066c885fdf"></a>`required`: `["service","status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d8ad4be00c"></a>`service` | yes | type="string"; minLength=1 |  |
| <a id="s-b5c365516c"></a>`status` | yes | type="string"; const="ok" |  |

## Governing policies

- <a id="pa-10f51bc576"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.HealthOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf81f9dc0e78d7ef3785f5b8317191c4184654d6eb2b611d352e3d8ed6e10062 -->

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
  "name": "HealthOut",
  "unit": "export"
}
```

</details>
