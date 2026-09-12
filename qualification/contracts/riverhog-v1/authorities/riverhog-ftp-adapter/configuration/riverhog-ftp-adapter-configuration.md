# riverhog-ftp-adapter configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:riverhog-ftp-adapter:riverhog-ftp-adapter-configuration:e46af825b1 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `configuration` |
| Family | `documents` |
| Contract elements | 1 |
| Extent decisions | 15 |

## Machine authority

- `/external_contract/configuration_documents/riverhog-ftp-adapter`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configuration-composition/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `configuration:riverhog-ftp-adapter` — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py::FtpAdapterConfig`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| encoded-size | bytes | `contract_max` | maximum=32768, reason=bounded-human-authored-catalog-description |
| length | characters | `contract_max` | maximum=32768, minimum=1, reason=schema-maximum |
| encoded-size | bytes | `contract_max` | maximum=65536, reason=bounded-human-authored-collection-tag |
| length | characters | `contract_max` | maximum=65536, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=512, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=255, minimum=1, reason=schema-maximum |
| value | schema-value | `contract_max` | maximum=3600, minimum=0.1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=255, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=2048, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |

## Contract

- `title`: FtpAdapterConfig
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `allow_insecure_http` | no | boolean |  |
| `api_token` | yes | string |  |
| `claim_attempt_budget` | no | integer |  |
| `completion_failure_attempt_budget` | no | integer |  |
| `completion_failure_capacity` | no | integer |  |
| `discovery_entry_budget` | no | integer |  |
| `host_id` | yes | string |  |
| `pending_claim_capacity` | no | integer |  |
| `poll_seconds` | no | number |  |
| `provenance_observer` | no | object (3 fields) |  |
| `riverhog_base_url` | yes | string |  |
| `riverhog_token` | yes | string |  |
| `sources` | yes | array |  |

### Definitions

| Definition | Shape |
|---|---|
| `CollectionDescription` | string |
| `CollectionTag` | string |
| `SourceConfig` | object |
