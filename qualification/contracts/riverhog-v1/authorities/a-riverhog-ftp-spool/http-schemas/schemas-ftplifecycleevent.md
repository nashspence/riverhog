# schemas: FtpLifecycleEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-ftplifecycleevent:821e6c3163 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-23cf774263"></a>

- <a id="s-ef3b056414"></a>`discriminator`: `{"mapping":{"io.riverhog.ftp_spool.claim.attempt_failed":"#/components/schemas/AttemptFailedEvent","io.riverhog.ftp_spool.claim.custody_ready":"#/components/schemas/CustodyReadyEvent","io.riverhog.ftp_spool.claim.published":"#/components/schemas/ClaimPublishedEvent","io.riverhog.ftp_spool.claim.registered":"#/components/schemas/ClaimRegisteredEvent"},"propertyName":"type"}`

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-e8616386c2"></a>1 | [ClaimRegisteredEvent](schemas-claimregisteredevent.md) |
| <a id="s-e49963992d"></a>2 | [CustodyReadyEvent](schemas-custodyreadyevent.md) |
| <a id="s-e67d5ae075"></a>3 | [AttemptFailedEvent](schemas-attemptfailedevent.md) |
| <a id="s-9edcf49c41"></a>4 | [ClaimPublishedEvent](schemas-claimpublishedevent.md) |

## Maintained corroboration

### Referenced contract elements

- [AttemptFailedEvent](schemas-attemptfailedevent.md)
- [ClaimPublishedEvent](schemas-claimpublishedevent.md)
- [ClaimRegisteredEvent](schemas-claimregisteredevent.md)
- [CustodyReadyEvent](schemas-custodyreadyevent.md)

## Governing policies

- <a id="pa-146ea5df88"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/FtpLifecycleEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9160b272cf5bb4eda536232eb91fb9d6986317a48b44c60609bee9d49cdfa33b -->

```json
{
  "discriminator": {
    "mapping": {
      "io.riverhog.ftp_spool.claim.attempt_failed": "#/components/schemas/AttemptFailedEvent",
      "io.riverhog.ftp_spool.claim.custody_ready": "#/components/schemas/CustodyReadyEvent",
      "io.riverhog.ftp_spool.claim.published": "#/components/schemas/ClaimPublishedEvent",
      "io.riverhog.ftp_spool.claim.registered": "#/components/schemas/ClaimRegisteredEvent"
    },
    "propertyName": "type"
  },
  "oneOf": [
    {
      "$ref": "#/components/schemas/ClaimRegisteredEvent"
    },
    {
      "$ref": "#/components/schemas/CustodyReadyEvent"
    },
    {
      "$ref": "#/components/schemas/AttemptFailedEvent"
    },
    {
      "$ref": "#/components/schemas/ClaimPublishedEvent"
    }
  ]
}
```

</details>
