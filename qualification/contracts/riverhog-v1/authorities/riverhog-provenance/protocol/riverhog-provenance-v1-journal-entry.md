# Riverhog provenance v1 journal entry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-provenance-v1-journal-entry:90e67ecf37 -->

One immutable entry in a hash-chained RFC 7464 per-file provenance journal.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-6adfbad66ec5) |
| Contract elements | 1 |
| Extent decisions | 79 |

## External contract

<a id="s-4efcfc32f77a"></a>
- <a id="s-d5bbb6637a9f"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json
- <a id="s-532513231bdd"></a>`title`: Riverhog provenance v1 journal entry
- <a id="s-3befa3a00a23"></a>`description`: One immutable entry in a hash-chained RFC 7464 per-file provenance journal.
- <a id="s-102bcf843f12"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-63c9dee775cc"></a>`$schema` | yes | const="https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json" |  |
| <a id="s-601285dd1617"></a>`body` | yes | type="object" |  |
| <a id="s-b4bb88b15aa9"></a>`entry_kind` | yes | type="string"; enum=["journal_init","assertion","correction","checkpoint"] |  |
| <a id="s-ffd569e30a8d"></a>`id` | yes | #/$defs/urnUuid |  |
| <a id="s-61ddb08d617a"></a>`journal_id` | yes | #/$defs/urnUuid |  |
| <a id="s-0bcb488ebad8"></a>`notes` | no | type="array"; minItems=1; items=(#/$defs/nonEmptyString); additional keys=`uniqueItems` |  |
| <a id="s-e2b6e7a7c7ab"></a>`previous_entry` | no | #/$defs/entryReference |  |
| <a id="s-b17b7abdd877"></a>`profile` | yes | const="https://nashspence.github.io/riverhog/v1/provenance" |  |
| <a id="s-dc9eae8f1352"></a>`recorded_at` | yes | #/$defs/utcDateTime |  |
| <a id="s-6bd89aa5b272"></a>`recorded_by_agent_id` | yes | #/$defs/urnUuid |  |
| <a id="s-b6dfb2d33164"></a>`recording_environment_id` | no | #/$defs/urnUuid |  |
| <a id="s-93c9c7970e1a"></a>`schema_version` | yes | const="1.0.0" |  |
| <a id="s-a755e6074ed8"></a>`sequence` | yes | type="integer"; minimum=0; maximum="9223372036854775807" |  |
| <a id="s-788bc03acebb"></a>`type` | yes | const="riverhog_provenance_journal_entry" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-088a6da7a641"></a>`absoluteUri` | type="string"; format="uri"; pattern="^[^\\u0000\\uD800-\\uDFFF]+$" |
| <a id="s-f7179075a7eb"></a>`accessMetadata` | type="object"; fields=`group`, `owner`, `posix_mode`; additional keys=`additionalProperties`, `minProperties` |
| <a id="s-afa0679572f6"></a>`activityTime` | type="object"; fields=`ended_at`, `note`, `started_at`, `status`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-73d177dbf5d4"></a>`agent` | oneOf=#/$defs/softwareAgent \| #/$defs/personAgent \| #/$defs/organizationAgent \| #/$defs/hardwareAgent |
| <a id="s-d348c1e87160"></a>`assertionBody` | type="object"; fields=`assertions`; additional keys=`additionalProperties`, `required` |
| <a id="s-bdc57712843b"></a>`association` | type="object"; fields=`agent_id`, `plan_id`, `role`, `role_uri`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-72c05fe31a47"></a>`booleanValue` | type="object"; fields=`data`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-f8865efa4de3"></a>`byteString` | type="object"; fields=`byte_length`, `data`, `digests`, `encoding`, `media_type`; additional keys=`additionalProperties`, `required` |
| <a id="s-57e550bbaff9"></a>`bytesValue` | type="object"; fields=`byte_length`, `data`, `digests`, `encoding`, `media_type`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-f1bbe8d25ccf"></a>`captureDetail` | type="object"; fields=`command_line`, `configuration_digest`, `profile_id`, `provenance_observer`, `working_directory`; additional keys=`additionalProperties`, `minProperties` |
| <a id="s-67bdd25fe2bb"></a>`captureEvent` | type="object"; fields=`associations`, `consistency`, `coverage`, `detail`, `diagnostics`, `ended_at`, `environment_id`, `id`, `notes`, `operations`, `outcome`, `started_at`, `state_id`, `type`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-14ff4aeeaf46"></a>`capturedValue` | oneOf=#/$defs/bytesValue \| #/$defs/textValue \| #/$defs/integerValue \| #/$defs/decimalValue \| #/$defs/booleanValue \| #/$defs/timestampValue \| #/$defs/uriValue \| #/$defs/entityReferenceValue \| #/$defs/jsonValue |
| <a id="s-edbb3f12655f"></a>`checkpointBody` | type="object"; fields=`checkpoint_kind`, `checkpoint_kind_uri`, `counts`, `covered_through`, `note`, `stream_prefix_sha256`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-62a0a91aaeb6"></a>`checkpointCounts` | type="object"; fields=`activities`, `captures`, `entries`, `lineages`, `relations`, `states`; additional keys=`additionalProperties`, `required` |
| <a id="s-956f3fc8b87a"></a>`comparisonDimension` | type="object"; fields=`basis`, `dimension`, `note`, `result`; additional keys=`additionalProperties`, `required` |
| <a id="s-b92aeb48b455"></a>`comparisonRelation` | type="object"; fields=`asserted_by_agent_id`, `compared_at`, `confidence`, `dimensions`, `from_capture_id`, `from_state`, `id`, `notes`, `to_capture_id`, `to_state`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-e5941ad1802c"></a>`contentDescription` | type="object"; fields=`digests`, `size_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-7cf5239b36f2"></a>`continuityBasis` | type="object"; fields=`type`, `uri`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-37e4bccab249"></a>`continuityRelation` | type="object"; fields=`asserted_by_agent_id`, `basis`, `confidence`, `continuity_kind`, `from_state`, `id`, `note`, `to_state`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-9293f3fd5ea4"></a>`correctionBody` | type="object"; fields=`action`, `reason`, `replacement`, `supersedes`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-ec3204909691"></a>`coverage` | type="object"; fields=`access_control`, `alternate_streams`, `basic_filesystem`, `content_fixity`, `extended_attributes`, `file_flags`, `locator`, `native_identifiers`, `native_metadata_other`, `ownership`, `permissions`, `resource_forks`, `security_metadata`, `special_file_features`, `storage_layout`, `timestamps`; additional keys=`additionalProperties`, `required` |
| <a id="s-e778f73563fc"></a>`coverageStatus` | type="string"; enum=["complete","partial","not_supported","not_applicable","not_requested","failed"] |
| <a id="s-24acd86cbadb"></a>`decimalValue` | type="object"; fields=`data`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-1d0efc506db2"></a>`derivationRelation` | type="object"; fields=`activity_id`, `derivation_kind`, `derivation_kind_uri`, `generated_state`, `id`, `type`, `used_state`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-5eb1052892cf"></a>`diagnostic` | type="object"; fields=`category`, `code`, `message`, `native_code`, `severity`, `source`; additional keys=`additionalProperties`, `required` |
| <a id="s-1e1e5e54b01a"></a>`digest` | type="object"; fields=`algorithm`, `algorithm_uri`, `encoding`, `originator_agent_id`, `purpose`, `purpose_uri`, `value`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-b87a395047c1"></a>`digestOnlyValue` | type="object"; fields=`byte_length`, `digests`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-47030d684de7"></a>`entityReferenceValue` | type="object"; fields=`data`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-4a73d347fd5a"></a>`entryReference` | type="object"; fields=`entry_id`, `json_sha256`, `sequence`; additional keys=`additionalProperties`, `required` |
| <a id="s-20b7cca542e4"></a>`environment` | type="object"; fields=`filesystem`, `host`, `id`, `operating_system`, `runtime`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-dd3ca68abf82"></a>`fieldSourceDescriptor` | allOf=#/$defs/sourceDescriptor \| additional keys=`required` |
| <a id="s-42ed3b06f06c"></a>`fileLineage` | type="object"; fields=`asserted_by_agent_id`, `continuity_basis`, `continuity_basis_uri`, `id`, `identifiers`, `label`, `notes`, `type`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-72ed1caeab6e"></a>`fileState` | type="object"; fields=`content`, `filesystem_metadata`, `id`, `lineage_id`, `locator`, `notes`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-92f8e87e3352"></a>`filesystem` | type="object"; fields=`case_preserving`, `case_sensitive`, `mount_locator`, `name_normalization`, `networked`, `snapshot_identifiers`, `type`, `type_uri`, `version`, `volume_identifiers`; additional keys=`additionalProperties`, `required` |
| <a id="s-2c6dad9ae676"></a>`filesystemMetadata` | type="object"; fields=`access`, `native_identifiers`, `native_metadata`, `timestamps`; additional keys=`additionalProperties`, `required` |
| <a id="s-fd99a1f0e8d4"></a>`generationRelation` | type="object"; fields=`activity_id`, `id`, `role`, `role_uri`, `state`, `type`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-b3f23dc21942"></a>`graphFragment` | type="object"; fields=`activities`, `agents`, `captures`, `environments`, `extensions`, `lineages`, `payload_bindings`, `relations`, `states`; additional keys=`additionalProperties`, `minProperties` |
| <a id="s-2d4ca24d8e9b"></a>`hardwareAgent` | type="object"; fields=`id`, `identifiers`, `model`, `name`, `type`, `vendor`, `version`; additional keys=`additionalProperties`, `required` |
| <a id="s-03a9361935ba"></a>`host` | type="object"; fields=`hardware_architecture`, `hardware_model`, `id`, `identifiers`, `name`; additional keys=`additionalProperties`, `required` |
| <a id="s-e6d4c3ce9412"></a>`identifier` | type="object"; fields=`authority_id`, `representation`, `scheme`, `scope`, `value`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-e7a29598b58e"></a>`integerValue` | type="object"; fields=`data`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-f201133ac44c"></a>`interpretation` | type="object"; fields=`agent_id`, `confidence`, `kind`, `note`, `schema`, `value`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-7be03ea94265"></a>`invalidationRelation` | type="object"; fields=`activity_id`, `id`, `reason`, `state`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-8b0ea6895864"></a>`journalInitBody` | type="object"; fields=`assertions`, `journal`; additional keys=`additionalProperties`, `required` |
| <a id="s-31e80eaaa268"></a>`journalPolicy` | type="object"; fields=`correction_model`, `entry_digest_algorithm`, `entry_digest_coverage`, `label`, `payload_semantics`, `primary_lineage_id`, `retention_intent`, `retention_intent_uri`, `scope`, `serialization`, `state_representation`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-461d5ccad63d"></a>`jsonValue` | type="object"; fields=`data`, `schema`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-2920056f4cc4"></a>`kernel` | type="object"; fields=`name`, `release`, `version`; additional keys=`additionalProperties`, `minProperties` |
| <a id="s-334ec6328c79"></a>`locator` | type="object"; fields=`authority_id`, `bytes`, `kind`, `source_encoding`, `syntax`, `text`, `text_role`; anyOf=additional keys=`required` \| additional keys=`required`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-cc468e7930dd"></a>`nativeCoverageCategory` | type="string"; enum=["extended_attributes","access_control","alternate_streams","resource_forks","file_flags","security_metadata","storage_layout","special_file_features","native_metadata_other"] |
| <a id="s-29df5b2baf7c"></a>`nativeMetadata` | type="object"; fields=`capture_status`, `coverage_category`, `interpretations`, `kind`, `kind_uri`, `name`, `name_bytes`, `name_role`, `name_source_encoding`, `namespace`, `note`, `observed_byte_length`, `sensitivity`, `source`, `value`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-ddcf450d4de8"></a>`nonEmptyString` | type="string"; minLength=1; pattern="^[^\\u0000\\uD800-\\uDFFF]+$" |
| <a id="s-899e81240515"></a>`nonNullJson` | anyOf=#/$defs/portableString \| type="number" \| type="boolean" \| type="array"; items=(#/$defs/nonNullJson) \| type="object"; additional keys=`additionalProperties`, `propertyNames` |
| <a id="s-325bfde37a61"></a>`observedIdentifier` | type="object"; fields=`authority_id`, `representation`, `scheme`, `scope`, `source`, `value`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-e585eafb762b"></a>`operatingSystem` | type="object"; fields=`build`, `family`, `family_name`, `identifiers`, `kernel`, `name`, `version`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-a050110c08e2"></a>`organizationAgent` | type="object"; fields=`id`, `identifiers`, `name`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-86c27faf36a2"></a>`payloadBinding` | type="object"; fields=`asserted_by_agent_id`, `basis`, `basis_uri`, `established_by_activity_id`, `established_by_capture_id`, `id`, `note`, `operation`, `relative_payload_locator`, `replaces_binding_id`, `role`, `role_uri`, `state`, `type`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-5d421881fe42"></a>`personAgent` | type="object"; fields=`id`, `identifiers`, `name`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-94d0b65f8181"></a>`portableString` | type="string"; pattern="^[^\\u0000\\uD800-\\uDFFF]*$" |
| <a id="s-dfb4db485fc8"></a>`principal` | type="object"; fields=`identifiers`, `kind`, `name`, `resolution`; anyOf=additional keys=`required` \| additional keys=`required`; additional keys=`additionalProperties`, `required` |
| <a id="s-05bd4b5b3355"></a>`processDetail` | type="object"; fields=`command_line`, `configuration_digest`, `external_event_identifier`, `plan_id`, `working_directory`; additional keys=`additionalProperties`, `minProperties` |
| <a id="s-9ccea05d102c"></a>`processEvidence` | type="object"; fields=`asserted_by_agent_id`, `basis`, `basis_uri`, `comparison_relation_id`, `confidence`, `description`, `id`, `reference`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-e81f29b9b5bd"></a>`provenanceContractReference` | type="object"; fields=`contract_id`, `contract_sha256`, `format`, `provider`; additional keys=`additionalProperties`, `required` |
| <a id="s-307ea8eb104a"></a>`provenanceObserverReference` | type="object"; fields=`contract`, `distribution`, `format`, `observer_id`, `provider`, `version`; additional keys=`additionalProperties`, `dependentRequired`, `required` |
| <a id="s-a65a100c0b5b"></a>`relation` | oneOf=#/$defs/usageRelation \| #/$defs/generationRelation \| #/$defs/derivationRelation \| #/$defs/invalidationRelation \| #/$defs/continuityRelation \| #/$defs/comparisonRelation |
| <a id="s-e8775f45df69"></a>`relationRole` | type="string"; enum=["source","input","reference","component","metadata_source","result","output","derivative","replacement","copy","other"] |
| <a id="s-a011de07f609"></a>`runtime` | type="object"; fields=`character_encoding`, `container`, `effective_principal`, `identifiers`, `locale`, `privilege`, `process_architecture`, `time_zone`, `utc_offset`; additional keys=`additionalProperties`, `required` |
| <a id="s-2d2165b5a6d7"></a>`semanticAssertion` | type="object"; fields=`asserted_by_agent_id`, `confidence`, `id`, `note`, `property`, `subject_id`, `type`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-1ceccb1b6145"></a>`sha256Hex` | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-e4f49d875899"></a>`softwareAgent` | type="object"; fields=`build`, `executable_digests`, `id`, `identifiers`, `name`, `type`, `vendor`, `version`; anyOf=additional keys=`required` \| additional keys=`required` \| additional keys=`required`; additional keys=`additionalProperties`, `required` |
| <a id="s-6bcbe0dfe1db"></a>`sourceDescriptor` | type="object"; fields=`api`, `api_uri`, `field`, `field_uri`, `platform`, `platform_name`, `version`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-e5152341be9f"></a>`sourcePlatform` | type="string"; enum=["linux","windows","macos","freebsd","openbsd","netbsd","illumos","aix","posix","android","ios","solaris","other"] |
| <a id="s-6bc47c0dba64"></a>`stateReference` | type="object"; fields=`entry_id`, `entry_json_sha256`, `id`, `journal_id`, `scope`, `sidecar_uri`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `dependentRequired`, `required` |
| <a id="s-ce6e164ba201"></a>`textValue` | type="object"; fields=`byte_length`, `data`, `language`, `media_type`, `source_encoding`, `type`; additional keys=`additionalProperties`, `dependentRequired`, `required` |
| <a id="s-f1f3653cef5b"></a>`timestampObservation` | type="object"; fields=`assumption`, `kind`, `kind_uri`, `raw_epoch`, `raw_unit`, `raw_unit_name`, `raw_value`, `resolution_ns`, `source`, `value`, `value_status`; allOf=additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then` \| additional keys=`if`, `then`; additional keys=`additionalProperties`, `dependentRequired`, `required` |
| <a id="s-9d92b09f3e8b"></a>`timestampValue` | type="object"; fields=`data`, `resolution_ns`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-95e37398dee1"></a>`transitionActivity` | type="object"; fields=`associations`, `detail`, `environment_id`, `event_label`, `event_type`, `event_type_uri`, `evidence`, `id`, `notes`, `outcome`, `time`, `type`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-de01b50b5cc9"></a>`typedValue` | oneOf=#/$defs/capturedValue \| #/$defs/digestOnlyValue |
| <a id="s-6dc893854935"></a>`uriValue` | type="object"; fields=`data`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-b9f9301bba02"></a>`urnUuid` | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" |
| <a id="s-2a66a85deba0"></a>`usageRelation` | type="object"; fields=`activity_id`, `id`, `role`, `role_uri`, `state`, `type`; allOf=additional keys=`if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-57f05b370e1d"></a>`utcDateTime` | type="string"; format="date-time"; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,9})?Z$" |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition accessMetadata](#s-f7179075a7eb) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-ebc2a4d88d44"></a>definition byteString · field digests | `cardinality · items · operational_policy` | shared above |
| <a id="s-4426948ec38a"></a>definition bytesValue · field digests | `cardinality · items · operational_policy` | shared above |
| <a id="s-635207689024"></a>definition captureDetail · field command_line | `cardinality · items · operational_policy` | shared above |
| [definition captureDetail](#s-f1bbe8d25ccf) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-42651f0d7d9b"></a>definition captureEvent · field associations | `cardinality · items · operational_policy` | shared above |
| <a id="s-e1676a2787bd"></a>definition captureEvent · field diagnostics | `cardinality · items · operational_policy` | shared above |
| <a id="s-9026ae9f306f"></a>definition captureEvent · field notes | `cardinality · items · operational_policy` | shared above |
| <a id="s-e61e291f8a18"></a>definition captureEvent · field operations | `cardinality · items · operational_policy` | shared above |
| <a id="s-f7e5cae39af7"></a>definition comparisonDimension · field basis | `cardinality · items · operational_policy` | shared above |
| <a id="s-089c1457eef9"></a>definition comparisonRelation · field dimensions | `cardinality · items · operational_policy` | shared above |
| <a id="s-7af1f6b6a557"></a>definition comparisonRelation · field notes | `cardinality · items · operational_policy` | shared above |
| <a id="s-d8a3c25d9d28"></a>definition contentDescription · field digests | `cardinality · items · operational_policy` | shared above |
| <a id="s-1396b618b32e"></a>definition continuityRelation · field basis | `cardinality · items · operational_policy` | shared above |
| <a id="s-49a4432a2c10"></a>definition correctionBody · field supersedes | `cardinality · items · operational_policy` | shared above |
| <a id="s-bf0abcdad053"></a>definition digestOnlyValue · field digests | `cardinality · items · operational_policy` | shared above |
| <a id="s-0d3b23ad9d42"></a>definition fileLineage · field identifiers | `cardinality · items · operational_policy` | shared above |
| <a id="s-7ea2d17ae93d"></a>definition fileLineage · field notes | `cardinality · items · operational_policy` | shared above |
| <a id="s-8a8b9a8e5089"></a>definition fileState · field notes | `cardinality · items · operational_policy` | shared above |
| <a id="s-3fa7d05459c9"></a>definition filesystem · field snapshot_identifiers | `cardinality · items · operational_policy` | shared above |
| <a id="s-93f4226725dc"></a>definition filesystem · field volume_identifiers | `cardinality · items · operational_policy` | shared above |
| <a id="s-85b269c7a3d0"></a>definition filesystemMetadata · field native_identifiers | `cardinality · items · operational_policy` | shared above |
| <a id="s-6957b5d6cbd6"></a>definition filesystemMetadata · field native_metadata | `cardinality · items · operational_policy` | shared above |
| <a id="s-10ce445ffba0"></a>definition filesystemMetadata · field timestamps | `cardinality · items · operational_policy` | shared above |
| <a id="s-4bc7c3210b3c"></a>definition graphFragment · field activities | `cardinality · items · operational_policy` | shared above |
| <a id="s-f8492f18ad1b"></a>definition graphFragment · field agents | `cardinality · items · operational_policy` | shared above |
| <a id="s-08b1a0dcefbc"></a>definition graphFragment · field captures | `cardinality · items · operational_policy` | shared above |
| <a id="s-3fdd6cd86d61"></a>definition graphFragment · field environments | `cardinality · items · operational_policy` | shared above |
| <a id="s-c242ca70cf52"></a>definition graphFragment · field extensions | `cardinality · items · operational_policy` | shared above |
| <a id="s-951061ad066d"></a>definition graphFragment · field lineages | `cardinality · items · operational_policy` | shared above |
| <a id="s-f7fd60d74593"></a>definition graphFragment · field payload_bindings | `cardinality · items · operational_policy` | shared above |
| <a id="s-02ccd37f7774"></a>definition graphFragment · field relations | `cardinality · items · operational_policy` | shared above |
| <a id="s-dc1fadd69b1a"></a>definition graphFragment · field states | `cardinality · items · operational_policy` | shared above |
| [definition graphFragment](#s-b3f23dc21942) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-08e920634ab2"></a>definition hardwareAgent · field identifiers | `cardinality · items · operational_policy` | shared above |
| <a id="s-f69daf11021d"></a>definition host · field identifiers | `cardinality · items · operational_policy` | shared above |
| [definition kernel](#s-2920056f4cc4) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-e99672219a3f"></a>definition nativeMetadata · field interpretations | `cardinality · items · operational_policy` | shared above |
| <a id="s-4bfccb267e78"></a>definition nonNullJson · anyOf alternative 4 | `cardinality · items · operational_policy` | shared above |
| <a id="s-b8d3cf55bde4"></a>definition nonNullJson · anyOf alternative 5 | `cardinality · entries · operational_policy` | shared above |
| <a id="s-9b913d3cb168"></a>definition operatingSystem · field identifiers | `cardinality · items · operational_policy` | shared above |
| <a id="s-992df3c10223"></a>definition organizationAgent · field identifiers | `cardinality · items · operational_policy` | shared above |
| <a id="s-2183d6d8e764"></a>definition personAgent · field identifiers | `cardinality · items · operational_policy` | shared above |
| <a id="s-64fe3643d1e5"></a>definition principal · field identifiers | `cardinality · items · operational_policy` | shared above |
| <a id="s-0469ac8a0a50"></a>definition processDetail · field command_line | `cardinality · items · operational_policy` | shared above |
| [definition processDetail](#s-05bd4b5b3355) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-c71255992291"></a>definition runtime · field identifiers | `cardinality · items · operational_policy` | shared above |
| <a id="s-a13bbfca5520"></a>definition softwareAgent · field executable_digests | `cardinality · items · operational_policy` | shared above |
| <a id="s-0524e1fcb70e"></a>definition softwareAgent · field identifiers | `cardinality · items · operational_policy` | shared above |
| <a id="s-982d0bba3901"></a>definition transitionActivity · field associations | `cardinality · items · operational_policy` | shared above |
| <a id="s-e93307925ccd"></a>definition transitionActivity · field evidence | `cardinality · items · operational_policy` | shared above |
| <a id="s-3606543f5e2a"></a>definition transitionActivity · field notes | `cardinality · items · operational_policy` | shared above |
| [field notes](#s-0bcb488ebad8) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-3d49292458a1"></a>definition accessMetadata · field posix_mode | `length · characters · fixed` | maximum=4; minimum=4; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-7]{4}$"} |
| <a id="s-20d5f4d688fe"></a>definition byteString · field byte_length | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-d7fe9175c0ac"></a>definition bytesValue · field byte_length | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-92b955b86b9c"></a>definition checkpointCounts · field activities | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-fd70682b5957"></a>definition checkpointCounts · field captures | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-c6970314634b"></a>definition checkpointCounts · field entries | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=1; reason="schema-maximum" |
| <a id="s-c3f33d3b7280"></a>definition checkpointCounts · field lineages | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-e41cbc72e40b"></a>definition checkpointCounts · field relations | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-917de38a38d4"></a>definition checkpointCounts · field states | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-26c2b67719f5"></a>definition contentDescription · field size_bytes | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-aca6a83557c1"></a>definition diagnostic · field code | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| <a id="s-1ab9b3b9fb11"></a>definition digest · allOf alternative 1 · then · field value | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-0cc851c7ddcc"></a>definition digest · allOf alternative 2 · then · field value | `length · characters · fixed` | maximum=128; minimum=128; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{128}$"} |
| <a id="s-c7446adbb4c4"></a>definition digestOnlyValue · field byte_length | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-f4eef917766d"></a>definition entryReference · field sequence | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-9f1b98348320"></a>definition filesystem · field type | `length · characters · contract_max` | maximum=128; minimum=1; reason="schema-maximum" |
| <a id="s-59c007d25a81"></a>definition identifier · allOf alternative 1 · then · field value | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-7b8bf71694be"></a>definition identifier · field scheme | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| <a id="s-b98cbebd2edc"></a>definition nativeMetadata · field observed_byte_length | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-11be07c3333d"></a>definition observedIdentifier · allOf alternative 1 · then · field value | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-7ffa2e848ba7"></a>definition observedIdentifier · field scheme | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [definition sha256Hex](#s-1ceccb1b6145) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-8748aa219f80"></a>definition textValue · field byte_length | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |
| <a id="s-f82ef2c6152e"></a>definition timestampObservation · field resolution_ns | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=1; reason="schema-maximum" |
| <a id="s-d37b49c3942e"></a>definition timestampValue · field resolution_ns | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=1; reason="schema-maximum" |
| [field sequence](#s-a755e6074ed8) | `value · schema-value · contract_max` | maximum="9223372036854775807"; minimum=0; reason="schema-maximum" |

## Governing policies

- <a id="pa-df1fb8cfb68d"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-926502819139"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-a43d3e5f5f87"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json](../../../evidence/sources.md#src-819610ba95e0) — `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-v1-journal-entry.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1journal-entry.schema.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b93f8c2bb1707ac9179e764afc163ed06036bf508fb7e9692dfb9930602e738 -->

```json
{
  "$defs": {
    "absoluteUri": {
      "format": "uri",
      "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
      "type": "string"
    },
    "accessMetadata": {
      "additionalProperties": false,
      "minProperties": 1,
      "properties": {
        "group": {
          "$ref": "#/$defs/principal"
        },
        "owner": {
          "$ref": "#/$defs/principal"
        },
        "posix_mode": {
          "pattern": "^[0-7]{4}$",
          "type": "string"
        }
      },
      "type": "object"
    },
    "activityTime": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "status": {
                "const": "exact"
              }
            },
            "required": [
              "status"
            ]
          },
          "then": {
            "required": [
              "started_at",
              "ended_at"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "status": {
                "const": "approximate"
              }
            },
            "required": [
              "status"
            ]
          },
          "then": {
            "anyOf": [
              {
                "required": [
                  "started_at"
                ]
              },
              {
                "required": [
                  "ended_at"
                ]
              }
            ],
            "required": [
              "note"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "status": {
                "const": "bounded"
              }
            },
            "required": [
              "status"
            ]
          },
          "then": {
            "required": [
              "started_at",
              "ended_at",
              "note"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "status": {
                "const": "unknown"
              }
            },
            "required": [
              "status"
            ]
          },
          "then": {
            "not": {
              "anyOf": [
                {
                  "required": [
                    "started_at"
                  ]
                },
                {
                  "required": [
                    "ended_at"
                  ]
                }
              ]
            },
            "required": [
              "note"
            ]
          }
        }
      ],
      "properties": {
        "ended_at": {
          "$ref": "#/$defs/utcDateTime"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "started_at": {
          "$ref": "#/$defs/utcDateTime"
        },
        "status": {
          "enum": [
            "exact",
            "approximate",
            "bounded",
            "unknown"
          ],
          "type": "string"
        }
      },
      "required": [
        "status"
      ],
      "type": "object"
    },
    "agent": {
      "oneOf": [
        {
          "$ref": "#/$defs/softwareAgent"
        },
        {
          "$ref": "#/$defs/personAgent"
        },
        {
          "$ref": "#/$defs/organizationAgent"
        },
        {
          "$ref": "#/$defs/hardwareAgent"
        }
      ]
    },
    "assertionBody": {
      "additionalProperties": false,
      "properties": {
        "assertions": {
          "$ref": "#/$defs/graphFragment"
        }
      },
      "required": [
        "assertions"
      ],
      "type": "object"
    },
    "association": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "role": {
                "const": "other"
              }
            },
            "required": [
              "role"
            ]
          },
          "then": {
            "required": [
              "role_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "role": {
                "const": "executing_software"
              }
            },
            "required": [
              "role"
            ]
          },
          "then": {
            "required": [
              "plan_id"
            ]
          }
        }
      ],
      "properties": {
        "agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "plan_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "role": {
          "enum": [
            "executing_software",
            "operator",
            "responsible_organization",
            "authorizing_agent",
            "capture_device",
            "process_owner",
            "creator",
            "editor",
            "reviewer",
            "other"
          ],
          "type": "string"
        },
        "role_uri": {
          "$ref": "#/$defs/absoluteUri"
        }
      },
      "required": [
        "agent_id",
        "role"
      ],
      "type": "object"
    },
    "booleanValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "type": "boolean"
        },
        "type": {
          "const": "boolean"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "byteString": {
      "additionalProperties": false,
      "properties": {
        "byte_length": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "data": {
          "pattern": "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$",
          "type": "string"
        },
        "digests": {
          "items": {
            "$ref": "#/$defs/digest"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "encoding": {
          "const": "base64"
        },
        "media_type": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "encoding",
        "data",
        "byte_length"
      ],
      "type": "object"
    },
    "bytesValue": {
      "additionalProperties": false,
      "properties": {
        "byte_length": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "data": {
          "pattern": "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$",
          "type": "string"
        },
        "digests": {
          "items": {
            "$ref": "#/$defs/digest"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "encoding": {
          "const": "base64"
        },
        "media_type": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "bytes"
        }
      },
      "required": [
        "type",
        "encoding",
        "data",
        "byte_length"
      ],
      "type": "object"
    },
    "captureDetail": {
      "additionalProperties": false,
      "minProperties": 1,
      "properties": {
        "command_line": {
          "items": {
            "$ref": "#/$defs/portableString"
          },
          "minItems": 1,
          "type": "array"
        },
        "configuration_digest": {
          "$ref": "#/$defs/digest"
        },
        "profile_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "provenance_observer": {
          "$ref": "#/$defs/provenanceObserverReference"
        },
        "working_directory": {
          "$ref": "#/$defs/locator"
        }
      },
      "type": "object"
    },
    "captureEvent": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "outcome": {
                "const": "partial"
              }
            },
            "required": [
              "outcome"
            ]
          },
          "then": {
            "required": [
              "diagnostics"
            ]
          }
        }
      ],
      "description": "Observation Activity/PREMIS Event that used a file state and technical environment to generate provenance assertions. It does not generate the file state.",
      "properties": {
        "associations": {
          "contains": {
            "properties": {
              "role": {
                "const": "executing_software"
              }
            },
            "required": [
              "role"
            ],
            "type": "object"
          },
          "items": {
            "$ref": "#/$defs/association"
          },
          "minContains": 1,
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "consistency": {
          "enum": [
            "snapshot",
            "verified_unchanged",
            "best_effort",
            "unknown"
          ],
          "type": "string"
        },
        "coverage": {
          "$ref": "#/$defs/coverage"
        },
        "detail": {
          "$ref": "#/$defs/captureDetail"
        },
        "diagnostics": {
          "items": {
            "$ref": "#/$defs/diagnostic"
          },
          "minItems": 1,
          "type": "array"
        },
        "ended_at": {
          "$ref": "#/$defs/utcDateTime"
        },
        "environment_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "notes": {
          "items": {
            "$ref": "#/$defs/nonEmptyString"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "operations": {
          "allOf": [
            {
              "contains": {
                "const": "metadata_extraction"
              },
              "minContains": 1
            },
            {
              "contains": {
                "const": "message_digest_calculation"
              },
              "minContains": 1
            }
          ],
          "items": {
            "enum": [
              "metadata_extraction",
              "message_digest_calculation",
              "principal_resolution"
            ],
            "type": "string"
          },
          "minItems": 2,
          "type": "array",
          "uniqueItems": true
        },
        "outcome": {
          "enum": [
            "success",
            "partial"
          ],
          "type": "string"
        },
        "started_at": {
          "$ref": "#/$defs/utcDateTime"
        },
        "state_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "type": {
          "const": "file_state_capture"
        }
      },
      "required": [
        "id",
        "type",
        "state_id",
        "operations",
        "started_at",
        "ended_at",
        "outcome",
        "consistency",
        "environment_id",
        "associations",
        "coverage"
      ],
      "type": "object"
    },
    "capturedValue": {
      "oneOf": [
        {
          "$ref": "#/$defs/bytesValue"
        },
        {
          "$ref": "#/$defs/textValue"
        },
        {
          "$ref": "#/$defs/integerValue"
        },
        {
          "$ref": "#/$defs/decimalValue"
        },
        {
          "$ref": "#/$defs/booleanValue"
        },
        {
          "$ref": "#/$defs/timestampValue"
        },
        {
          "$ref": "#/$defs/uriValue"
        },
        {
          "$ref": "#/$defs/entityReferenceValue"
        },
        {
          "$ref": "#/$defs/jsonValue"
        }
      ]
    },
    "checkpointBody": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "checkpoint_kind": {
                "const": "other"
              }
            },
            "required": [
              "checkpoint_kind"
            ]
          },
          "then": {
            "required": [
              "checkpoint_kind_uri"
            ]
          }
        }
      ],
      "description": "A cumulative checkpoint over exact RFC 7464 bytes through the preceding entry. Tail truncation is detectable only when a checkpoint or tail digest is anchored externally.",
      "properties": {
        "checkpoint_kind": {
          "enum": [
            "periodic",
            "transfer",
            "archive_ingest",
            "publication",
            "other"
          ],
          "type": "string"
        },
        "checkpoint_kind_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "counts": {
          "$ref": "#/$defs/checkpointCounts"
        },
        "covered_through": {
          "$ref": "#/$defs/entryReference"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "stream_prefix_sha256": {
          "$ref": "#/$defs/sha256Hex"
        }
      },
      "required": [
        "checkpoint_kind",
        "covered_through",
        "stream_prefix_sha256",
        "counts"
      ],
      "type": "object"
    },
    "checkpointCounts": {
      "additionalProperties": false,
      "properties": {
        "activities": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "captures": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "entries": {
          "maximum": "9223372036854775807",
          "minimum": 1,
          "type": "integer"
        },
        "lineages": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "relations": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "states": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "entries"
      ],
      "type": "object"
    },
    "comparisonDimension": {
      "additionalProperties": false,
      "properties": {
        "basis": {
          "items": {
            "enum": [
              "size",
              "sha-256",
              "exact_json_value",
              "byte_exact",
              "normalized_value",
              "complete_capture",
              "producer_assertion"
            ],
            "type": "string"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "dimension": {
          "enum": [
            "primary_content",
            "locator",
            "timestamps",
            "ownership",
            "permissions",
            "native_identifiers",
            "native_metadata"
          ],
          "type": "string"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "result": {
          "enum": [
            "matching_fixity",
            "equal",
            "different",
            "not_observed",
            "not_comparable",
            "indeterminate"
          ],
          "type": "string"
        }
      },
      "required": [
        "dimension",
        "result",
        "basis"
      ],
      "type": "object"
    },
    "comparisonRelation": {
      "additionalProperties": false,
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "compared_at": {
          "$ref": "#/$defs/utcDateTime"
        },
        "confidence": {
          "enum": [
            "high",
            "medium",
            "low",
            "unknown"
          ],
          "type": "string"
        },
        "dimensions": {
          "items": {
            "$ref": "#/$defs/comparisonDimension"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "from_capture_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "from_state": {
          "$ref": "#/$defs/stateReference"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "notes": {
          "items": {
            "$ref": "#/$defs/nonEmptyString"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "to_capture_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "to_state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "state_comparison"
        }
      },
      "required": [
        "id",
        "type",
        "from_state",
        "to_state",
        "from_capture_id",
        "to_capture_id",
        "compared_at",
        "dimensions",
        "asserted_by_agent_id",
        "confidence"
      ],
      "type": "object"
    },
    "contentDescription": {
      "additionalProperties": false,
      "description": "Opaque primary byte-stream observations only: length and cryptographic fixity. Internal format, codec, dimensions, duration, and other byte semantics are outside the core profile.",
      "properties": {
        "digests": {
          "contains": {
            "properties": {
              "algorithm": {
                "const": "sha-256"
              },
              "purpose": {
                "const": "fixity"
              }
            },
            "required": [
              "algorithm",
              "purpose"
            ],
            "type": "object"
          },
          "items": {
            "$ref": "#/$defs/digest"
          },
          "maxContains": 1,
          "minContains": 1,
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "size_bytes": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "size_bytes",
        "digests"
      ],
      "type": "object"
    },
    "continuityBasis": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "type": {
                "const": "other"
              }
            },
            "required": [
              "type"
            ]
          },
          "then": {
            "required": [
              "uri"
            ]
          }
        }
      ],
      "properties": {
        "type": {
          "enum": [
            "native_file_identity",
            "snapshot_identity",
            "repository_tracking",
            "process_record",
            "operator_assertion",
            "matching_fixity_and_locator",
            "other"
          ],
          "type": "string"
        },
        "uri": {
          "$ref": "#/$defs/absoluteUri"
        }
      },
      "required": [
        "type"
      ],
      "type": "object"
    },
    "continuityRelation": {
      "additionalProperties": false,
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "basis": {
          "items": {
            "$ref": "#/$defs/continuityBasis"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "confidence": {
          "enum": [
            "high",
            "medium",
            "low",
            "unknown"
          ],
          "type": "string"
        },
        "continuity_kind": {
          "enum": [
            "reobservation",
            "same_source_instance",
            "declared_successor"
          ],
          "type": "string"
        },
        "from_state": {
          "$ref": "#/$defs/stateReference"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "to_state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "state_continuity"
        }
      },
      "required": [
        "id",
        "type",
        "from_state",
        "to_state",
        "continuity_kind",
        "basis",
        "asserted_by_agent_id",
        "confidence"
      ],
      "type": "object"
    },
    "correctionBody": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "action": {
                "const": "replace"
              }
            },
            "required": [
              "action"
            ]
          },
          "then": {
            "required": [
              "replacement"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "action": {
                "const": "retract"
              }
            },
            "required": [
              "action"
            ]
          },
          "then": {
            "not": {
              "required": [
                "replacement"
              ]
            }
          }
        }
      ],
      "description": "Monotonic correction of prior assertion entries. Supersession changes the effective provenance description; it never denotes a change to the file itself.",
      "properties": {
        "action": {
          "enum": [
            "replace",
            "retract"
          ],
          "type": "string"
        },
        "reason": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "replacement": {
          "$ref": "#/$defs/graphFragment"
        },
        "supersedes": {
          "items": {
            "$ref": "#/$defs/entryReference"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        }
      },
      "required": [
        "action",
        "reason",
        "supersedes"
      ],
      "type": "object"
    },
    "coverage": {
      "additionalProperties": false,
      "description": "Exhaustiveness status for every filesystem capture category. Complete concerns accountable enumeration under the declared plan, not universal retention.",
      "properties": {
        "access_control": {
          "$ref": "#/$defs/coverageStatus"
        },
        "alternate_streams": {
          "$ref": "#/$defs/coverageStatus"
        },
        "basic_filesystem": {
          "const": "complete"
        },
        "content_fixity": {
          "const": "complete"
        },
        "extended_attributes": {
          "$ref": "#/$defs/coverageStatus"
        },
        "file_flags": {
          "$ref": "#/$defs/coverageStatus"
        },
        "locator": {
          "const": "complete"
        },
        "native_identifiers": {
          "$ref": "#/$defs/coverageStatus"
        },
        "native_metadata_other": {
          "$ref": "#/$defs/coverageStatus"
        },
        "ownership": {
          "$ref": "#/$defs/coverageStatus"
        },
        "permissions": {
          "$ref": "#/$defs/coverageStatus"
        },
        "resource_forks": {
          "$ref": "#/$defs/coverageStatus"
        },
        "security_metadata": {
          "$ref": "#/$defs/coverageStatus"
        },
        "special_file_features": {
          "$ref": "#/$defs/coverageStatus"
        },
        "storage_layout": {
          "$ref": "#/$defs/coverageStatus"
        },
        "timestamps": {
          "$ref": "#/$defs/coverageStatus"
        }
      },
      "required": [
        "content_fixity",
        "locator",
        "basic_filesystem",
        "timestamps",
        "ownership",
        "permissions",
        "native_identifiers",
        "extended_attributes",
        "access_control",
        "alternate_streams",
        "resource_forks",
        "file_flags",
        "security_metadata",
        "storage_layout",
        "special_file_features",
        "native_metadata_other"
      ],
      "type": "object"
    },
    "coverageStatus": {
      "enum": [
        "complete",
        "partial",
        "not_supported",
        "not_applicable",
        "not_requested",
        "failed"
      ],
      "type": "string"
    },
    "decimalValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "pattern": "^-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?$",
          "type": "string"
        },
        "type": {
          "const": "decimal"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "derivationRelation": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "derivation_kind": {
                "const": "other"
              }
            },
            "required": [
              "derivation_kind"
            ]
          },
          "then": {
            "required": [
              "derivation_kind_uri"
            ]
          }
        }
      ],
      "properties": {
        "activity_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "derivation_kind": {
          "enum": [
            "revision",
            "transformation",
            "copy",
            "metadata_change",
            "relocation",
            "aggregation",
            "extraction",
            "other"
          ],
          "type": "string"
        },
        "derivation_kind_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "generated_state": {
          "$ref": "#/$defs/stateReference"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "type": {
          "const": "derivation"
        },
        "used_state": {
          "$ref": "#/$defs/stateReference"
        }
      },
      "required": [
        "id",
        "type",
        "generated_state",
        "used_state",
        "activity_id",
        "derivation_kind"
      ],
      "type": "object"
    },
    "diagnostic": {
      "additionalProperties": false,
      "properties": {
        "category": {
          "enum": [
            "general",
            "content_fixity",
            "locator",
            "basic_filesystem",
            "timestamps",
            "ownership",
            "permissions",
            "native_identifiers",
            "extended_attributes",
            "access_control",
            "alternate_streams",
            "resource_forks",
            "file_flags",
            "security_metadata",
            "storage_layout",
            "special_file_features",
            "native_metadata_other"
          ],
          "type": "string"
        },
        "code": {
          "maxLength": 255,
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        },
        "message": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "native_code": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "severity": {
          "enum": [
            "warning",
            "error"
          ],
          "type": "string"
        },
        "source": {
          "$ref": "#/$defs/sourceDescriptor"
        }
      },
      "required": [
        "severity",
        "category",
        "code",
        "message"
      ],
      "type": "object"
    },
    "digest": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "algorithm": {
                "const": "sha-256"
              }
            },
            "required": [
              "algorithm"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "algorithm": {
                "const": "sha-512"
              }
            },
            "required": [
              "algorithm"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "pattern": "^[0-9a-f]{128}$",
                "type": "string"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "algorithm": {
                "not": {
                  "enum": [
                    "sha-256",
                    "sha-512"
                  ]
                }
              }
            },
            "required": [
              "algorithm"
            ]
          },
          "then": {
            "required": [
              "algorithm_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "purpose": {
                "const": "other"
              }
            },
            "required": [
              "purpose"
            ]
          },
          "then": {
            "required": [
              "purpose_uri"
            ]
          }
        }
      ],
      "description": "A cryptographic digest assertion. Content digests identify byte equality evidence, not file-entity identity.",
      "properties": {
        "algorithm": {
          "pattern": "^[a-z0-9][a-z0-9.-]{0,63}$",
          "type": "string"
        },
        "algorithm_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "encoding": {
          "const": "hex"
        },
        "originator_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "purpose": {
          "enum": [
            "fixity",
            "native_metadata",
            "executable",
            "configuration",
            "evidence",
            "journal_entry",
            "other"
          ],
          "type": "string"
        },
        "purpose_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "value": {
          "pattern": "^[0-9a-f]+$",
          "type": "string"
        }
      },
      "required": [
        "algorithm",
        "encoding",
        "value",
        "purpose",
        "originator_agent_id"
      ],
      "type": "object"
    },
    "digestOnlyValue": {
      "additionalProperties": false,
      "properties": {
        "byte_length": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "digests": {
          "items": {
            "$ref": "#/$defs/digest"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "type": {
          "const": "digest"
        }
      },
      "required": [
        "type",
        "byte_length",
        "digests"
      ],
      "type": "object"
    },
    "entityReferenceValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/absoluteUri"
        },
        "type": {
          "const": "entity_reference"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "entryReference": {
      "additionalProperties": false,
      "description": "Reference to the exact UTF-8 JSON-text octets of an earlier journal entry, excluding RFC 7464 RS/LF framing.",
      "properties": {
        "entry_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "json_sha256": {
          "$ref": "#/$defs/sha256Hex"
        },
        "sequence": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "entry_id",
        "sequence",
        "json_sha256"
      ],
      "type": "object"
    },
    "environment": {
      "additionalProperties": false,
      "description": "Host/OS/filesystem/runtime state used by an observation or transition; a PROV Entity, not an Agent merely because execution occurred there.",
      "properties": {
        "filesystem": {
          "$ref": "#/$defs/filesystem"
        },
        "host": {
          "$ref": "#/$defs/host"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "operating_system": {
          "$ref": "#/$defs/operatingSystem"
        },
        "runtime": {
          "$ref": "#/$defs/runtime"
        },
        "type": {
          "const": "technical_environment"
        }
      },
      "required": [
        "id",
        "type",
        "host",
        "operating_system",
        "filesystem",
        "runtime"
      ],
      "type": "object"
    },
    "fieldSourceDescriptor": {
      "allOf": [
        {
          "$ref": "#/$defs/sourceDescriptor"
        },
        {
          "required": [
            "field"
          ]
        }
      ]
    },
    "fileLineage": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "continuity_basis": {
                "const": "other"
              }
            },
            "required": [
              "continuity_basis"
            ]
          },
          "then": {
            "required": [
              "continuity_basis_uri"
            ]
          }
        }
      ],
      "description": "An abstract continuing file lineage. Each immutable state is a specialization of one lineage; grouping is an attributable continuity assertion, not inferred from path or digest alone.",
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "continuity_basis": {
          "enum": [
            "producer_declared",
            "repository_tracking",
            "source_native_tracking",
            "process_provenance",
            "other"
          ],
          "type": "string"
        },
        "continuity_basis_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "label": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "notes": {
          "items": {
            "$ref": "#/$defs/nonEmptyString"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "type": {
          "const": "file_lineage"
        }
      },
      "required": [
        "id",
        "type",
        "continuity_basis",
        "asserted_by_agent_id"
      ],
      "type": "object"
    },
    "fileState": {
      "additionalProperties": false,
      "description": "One immutable, full observed snapshot of one operating-system regular-file state; a PROV Entity and PREMIS File.",
      "properties": {
        "content": {
          "$ref": "#/$defs/contentDescription"
        },
        "filesystem_metadata": {
          "$ref": "#/$defs/filesystemMetadata"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "lineage_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "locator": {
          "$ref": "#/$defs/locator"
        },
        "notes": {
          "items": {
            "$ref": "#/$defs/nonEmptyString"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "type": {
          "const": "regular_file_state"
        }
      },
      "required": [
        "id",
        "type",
        "lineage_id",
        "locator",
        "content",
        "filesystem_metadata"
      ],
      "type": "object"
    },
    "filesystem": {
      "additionalProperties": false,
      "properties": {
        "case_preserving": {
          "type": "boolean"
        },
        "case_sensitive": {
          "type": "boolean"
        },
        "mount_locator": {
          "$ref": "#/$defs/locator"
        },
        "name_normalization": {
          "enum": [
            "none",
            "nfc",
            "nfd",
            "nfkc",
            "nfkd",
            "implementation_defined",
            "unknown"
          ],
          "type": "string"
        },
        "networked": {
          "type": "boolean"
        },
        "snapshot_identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "type": {
          "maxLength": 128,
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        },
        "type_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "volume_identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        }
      },
      "required": [
        "type"
      ],
      "type": "object"
    },
    "filesystemMetadata": {
      "additionalProperties": false,
      "properties": {
        "access": {
          "$ref": "#/$defs/accessMetadata"
        },
        "native_identifiers": {
          "items": {
            "$ref": "#/$defs/observedIdentifier"
          },
          "type": "array",
          "uniqueItems": true
        },
        "native_metadata": {
          "items": {
            "$ref": "#/$defs/nativeMetadata"
          },
          "type": "array",
          "uniqueItems": true
        },
        "timestamps": {
          "items": {
            "$ref": "#/$defs/timestampObservation"
          },
          "type": "array",
          "uniqueItems": true
        }
      },
      "required": [
        "timestamps",
        "native_identifiers",
        "native_metadata"
      ],
      "type": "object"
    },
    "generationRelation": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "role": {
                "const": "other"
              }
            },
            "required": [
              "role"
            ]
          },
          "then": {
            "required": [
              "role_uri"
            ]
          }
        }
      ],
      "properties": {
        "activity_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "role": {
          "$ref": "#/$defs/relationRole"
        },
        "role_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "generation"
        }
      },
      "required": [
        "id",
        "type",
        "activity_id",
        "state",
        "role"
      ],
      "type": "object"
    },
    "graphFragment": {
      "additionalProperties": false,
      "description": "An immutable atomic bundle of graph assertions. Arrays are semantic sets; array order does not imply chronology or causality.",
      "minProperties": 1,
      "properties": {
        "activities": {
          "items": {
            "$ref": "#/$defs/transitionActivity"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "agents": {
          "items": {
            "$ref": "#/$defs/agent"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "captures": {
          "items": {
            "$ref": "#/$defs/captureEvent"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "environments": {
          "items": {
            "$ref": "#/$defs/environment"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "extensions": {
          "items": {
            "$ref": "#/$defs/semanticAssertion"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "lineages": {
          "items": {
            "$ref": "#/$defs/fileLineage"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "payload_bindings": {
          "items": {
            "$ref": "#/$defs/payloadBinding"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "relations": {
          "items": {
            "$ref": "#/$defs/relation"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "states": {
          "items": {
            "$ref": "#/$defs/fileState"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        }
      },
      "type": "object"
    },
    "hardwareAgent": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "model": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "hardware"
        },
        "vendor": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "id",
        "type",
        "name"
      ],
      "type": "object"
    },
    "host": {
      "additionalProperties": false,
      "properties": {
        "hardware_architecture": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "hardware_model": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "id"
      ],
      "type": "object"
    },
    "identifier": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "representation": {
                "enum": [
                  "sha256",
                  "hmac_sha256"
                ]
              }
            },
            "required": [
              "representation"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "scope": {
                "not": {
                  "const": "global"
                }
              }
            },
            "required": [
              "scope"
            ]
          },
          "then": {
            "required": [
              "authority_id"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "representation": {
                "const": "hmac_sha256"
              }
            },
            "required": [
              "representation"
            ]
          },
          "then": {
            "required": [
              "authority_id"
            ]
          }
        }
      ],
      "properties": {
        "authority_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "representation": {
          "enum": [
            "clear",
            "sha256",
            "hmac_sha256"
          ],
          "type": "string"
        },
        "scheme": {
          "maxLength": 255,
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        },
        "scope": {
          "enum": [
            "record",
            "repository",
            "host",
            "volume",
            "filesystem",
            "organization",
            "global"
          ],
          "type": "string"
        },
        "value": {
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        }
      },
      "required": [
        "scheme",
        "value",
        "scope",
        "representation"
      ],
      "type": "object"
    },
    "integerValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "pattern": "^-?(?:0|[1-9][0-9]*)$",
          "type": "string"
        },
        "type": {
          "const": "integer"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "interpretation": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "kind": {
                "const": "text_decode"
              }
            },
            "required": [
              "kind"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "$ref": "#/$defs/textValue"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "kind": {
                "enum": [
                  "structured_parse",
                  "other"
                ]
              }
            },
            "required": [
              "kind"
            ]
          },
          "then": {
            "required": [
              "schema"
            ]
          }
        }
      ],
      "properties": {
        "agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "confidence": {
          "enum": [
            "high",
            "medium",
            "low",
            "unknown"
          ],
          "type": "string"
        },
        "kind": {
          "enum": [
            "text_decode",
            "structured_parse",
            "normalized_value",
            "other"
          ],
          "type": "string"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "schema": {
          "$ref": "#/$defs/absoluteUri"
        },
        "value": {
          "$ref": "#/$defs/capturedValue"
        }
      },
      "required": [
        "kind",
        "value",
        "agent_id",
        "confidence"
      ],
      "type": "object"
    },
    "invalidationRelation": {
      "additionalProperties": false,
      "properties": {
        "activity_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "reason": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "invalidation"
        }
      },
      "required": [
        "id",
        "type",
        "activity_id",
        "state",
        "reason"
      ],
      "type": "object"
    },
    "journalInitBody": {
      "additionalProperties": false,
      "properties": {
        "assertions": {
          "$ref": "#/$defs/graphFragment"
        },
        "journal": {
          "$ref": "#/$defs/journalPolicy"
        }
      },
      "required": [
        "journal",
        "assertions"
      ],
      "type": "object"
    },
    "journalPolicy": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "retention_intent": {
                "const": "other"
              }
            },
            "required": [
              "retention_intent"
            ]
          },
          "then": {
            "required": [
              "retention_intent_uri"
            ]
          }
        }
      ],
      "properties": {
        "correction_model": {
          "const": "monotonic_entry_supersession"
        },
        "entry_digest_algorithm": {
          "const": "sha-256"
        },
        "entry_digest_coverage": {
          "const": "json_text_octets_excluding_framing"
        },
        "label": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "payload_semantics": {
          "const": "opaque_bytes"
        },
        "primary_lineage_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "retention_intent": {
          "enum": [
            "permanent_archival",
            "long_term_preservation",
            "operational_provenance",
            "other"
          ],
          "type": "string"
        },
        "retention_intent_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "scope": {
          "const": "primary_lineage_with_related_provenance"
        },
        "serialization": {
          "const": "rfc7464_json_text_sequence"
        },
        "state_representation": {
          "const": "full_snapshot"
        }
      },
      "required": [
        "primary_lineage_id",
        "scope",
        "serialization",
        "entry_digest_algorithm",
        "entry_digest_coverage",
        "state_representation",
        "payload_semantics",
        "correction_model",
        "retention_intent"
      ],
      "type": "object"
    },
    "jsonValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/nonNullJson"
        },
        "schema": {
          "$ref": "#/$defs/absoluteUri"
        },
        "type": {
          "const": "json"
        }
      },
      "required": [
        "type",
        "data",
        "schema"
      ],
      "type": "object"
    },
    "kernel": {
      "additionalProperties": false,
      "minProperties": 1,
      "properties": {
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "release": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "type": "object"
    },
    "locator": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "syntax": {
                "const": "opaque"
              }
            },
            "required": [
              "syntax"
            ]
          },
          "then": {
            "properties": {
              "kind": {
                "const": "opaque"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "kind": {
                "const": "opaque"
              }
            },
            "required": [
              "kind"
            ]
          },
          "then": {
            "properties": {
              "syntax": {
                "const": "opaque"
              }
            }
          }
        },
        {
          "if": {
            "required": [
              "text_role"
            ]
          },
          "then": {
            "required": [
              "text"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "text_role": {
                "const": "display"
              }
            },
            "required": [
              "text_role"
            ]
          },
          "then": {
            "required": [
              "bytes"
            ]
          }
        },
        {
          "if": {
            "required": [
              "text",
              "bytes"
            ]
          },
          "then": {
            "required": [
              "text_role"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "text_role": {
                "const": "exact"
              }
            },
            "required": [
              "text_role",
              "bytes"
            ]
          },
          "then": {
            "required": [
              "source_encoding"
            ]
          }
        }
      ],
      "anyOf": [
        {
          "required": [
            "text"
          ]
        },
        {
          "required": [
            "bytes"
          ]
        }
      ],
      "properties": {
        "authority_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "bytes": {
          "$ref": "#/$defs/byteString"
        },
        "kind": {
          "enum": [
            "absolute",
            "relative",
            "opaque"
          ],
          "type": "string"
        },
        "source_encoding": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "syntax": {
          "enum": [
            "posix",
            "windows",
            "uri",
            "opaque"
          ],
          "type": "string"
        },
        "text": {
          "$ref": "#/$defs/portableString"
        },
        "text_role": {
          "description": "Required when text and bytes coexist.",
          "enum": [
            "exact",
            "display"
          ],
          "type": "string"
        }
      },
      "required": [
        "syntax",
        "kind"
      ],
      "type": "object"
    },
    "nativeCoverageCategory": {
      "enum": [
        "extended_attributes",
        "access_control",
        "alternate_streams",
        "resource_forks",
        "file_flags",
        "security_metadata",
        "storage_layout",
        "special_file_features",
        "native_metadata_other"
      ],
      "type": "string"
    },
    "nativeMetadata": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "kind": {
                "const": "other"
              }
            },
            "required": [
              "kind"
            ]
          },
          "then": {
            "required": [
              "kind_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "capture_status": {
                "const": "captured"
              }
            },
            "required": [
              "capture_status"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "$ref": "#/$defs/capturedValue"
              }
            },
            "required": [
              "value"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "capture_status": {
                "const": "digest_only"
              }
            },
            "required": [
              "capture_status"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "$ref": "#/$defs/digestOnlyValue"
              }
            },
            "required": [
              "value"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "capture_status": {
                "enum": [
                  "redacted",
                  "unreadable",
                  "not_retained"
                ]
              }
            },
            "required": [
              "capture_status"
            ]
          },
          "then": {
            "not": {
              "required": [
                "value"
              ]
            }
          }
        },
        {
          "if": {
            "required": [
              "name_bytes"
            ]
          },
          "then": {
            "required": [
              "name_role"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "name_role": {
                "const": "display"
              }
            },
            "required": [
              "name_role"
            ]
          },
          "then": {
            "required": [
              "name_bytes"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "name_role": {
                "const": "exact"
              }
            },
            "required": [
              "name_role",
              "name_bytes"
            ]
          },
          "then": {
            "required": [
              "name_source_encoding"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "capture_status": {
                "enum": [
                  "redacted",
                  "unreadable",
                  "not_retained"
                ]
              }
            },
            "required": [
              "capture_status"
            ]
          },
          "then": {
            "required": [
              "note"
            ]
          }
        }
      ],
      "description": "Source-native metadata evidence separated from any interpretation and from restoration semantics.",
      "properties": {
        "capture_status": {
          "enum": [
            "captured",
            "digest_only",
            "redacted",
            "unreadable",
            "not_retained"
          ],
          "type": "string"
        },
        "coverage_category": {
          "$ref": "#/$defs/nativeCoverageCategory"
        },
        "interpretations": {
          "items": {
            "$ref": "#/$defs/interpretation"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "kind": {
          "enum": [
            "extended_attribute",
            "windows_extended_attribute",
            "alternate_data_stream",
            "resource_fork",
            "finder_info",
            "acl",
            "security_descriptor",
            "file_flag",
            "capability",
            "security_label",
            "reparse_point",
            "sparse_map",
            "compression_state",
            "encryption_state",
            "native_stat_field",
            "other"
          ],
          "type": "string"
        },
        "kind_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "name_bytes": {
          "$ref": "#/$defs/byteString"
        },
        "name_role": {
          "enum": [
            "exact",
            "display"
          ],
          "type": "string"
        },
        "name_source_encoding": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "namespace": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "observed_byte_length": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "sensitivity": {
          "enum": [
            "public",
            "personal",
            "security_sensitive",
            "secret",
            "unknown"
          ],
          "type": "string"
        },
        "source": {
          "$ref": "#/$defs/sourceDescriptor"
        },
        "value": {
          "$ref": "#/$defs/typedValue"
        }
      },
      "required": [
        "kind",
        "coverage_category",
        "name",
        "capture_status",
        "source",
        "sensitivity"
      ],
      "type": "object"
    },
    "nonEmptyString": {
      "minLength": 1,
      "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
      "type": "string"
    },
    "nonNullJson": {
      "anyOf": [
        {
          "$ref": "#/$defs/portableString"
        },
        {
          "type": "number"
        },
        {
          "type": "boolean"
        },
        {
          "items": {
            "$ref": "#/$defs/nonNullJson"
          },
          "type": "array"
        },
        {
          "additionalProperties": {
            "$ref": "#/$defs/nonNullJson"
          },
          "propertyNames": {
            "$ref": "#/$defs/portableString"
          },
          "type": "object"
        }
      ]
    },
    "observedIdentifier": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "representation": {
                "enum": [
                  "sha256",
                  "hmac_sha256"
                ]
              }
            },
            "required": [
              "representation"
            ]
          },
          "then": {
            "properties": {
              "value": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "scope": {
                "not": {
                  "const": "global"
                }
              }
            },
            "required": [
              "scope"
            ]
          },
          "then": {
            "required": [
              "authority_id"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "representation": {
                "const": "hmac_sha256"
              }
            },
            "required": [
              "representation"
            ]
          },
          "then": {
            "required": [
              "authority_id"
            ]
          }
        }
      ],
      "properties": {
        "authority_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "representation": {
          "enum": [
            "clear",
            "sha256",
            "hmac_sha256"
          ],
          "type": "string"
        },
        "scheme": {
          "maxLength": 255,
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        },
        "scope": {
          "enum": [
            "record",
            "repository",
            "host",
            "volume",
            "filesystem",
            "organization",
            "global"
          ],
          "type": "string"
        },
        "source": {
          "$ref": "#/$defs/sourceDescriptor"
        },
        "value": {
          "minLength": 1,
          "pattern": "^[^\\u0000\\uD800-\\uDFFF]+$",
          "type": "string"
        }
      },
      "required": [
        "scheme",
        "value",
        "scope",
        "representation",
        "source"
      ],
      "type": "object"
    },
    "operatingSystem": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "family": {
                "const": "other"
              }
            },
            "required": [
              "family"
            ]
          },
          "then": {
            "required": [
              "family_name"
            ]
          }
        }
      ],
      "properties": {
        "build": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "family": {
          "enum": [
            "linux",
            "windows",
            "macos",
            "freebsd",
            "openbsd",
            "netbsd",
            "illumos",
            "aix",
            "android",
            "ios",
            "solaris",
            "other"
          ],
          "type": "string"
        },
        "family_name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "kernel": {
          "$ref": "#/$defs/kernel"
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "family",
        "name",
        "version"
      ],
      "type": "object"
    },
    "organizationAgent": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "organization"
        }
      },
      "required": [
        "id",
        "type"
      ],
      "type": "object"
    },
    "payloadBinding": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "role": {
                "const": "other"
              }
            },
            "required": [
              "role"
            ]
          },
          "then": {
            "required": [
              "role_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "operation": {
                "const": "bind"
              }
            },
            "required": [
              "operation"
            ]
          },
          "then": {
            "oneOf": [
              {
                "required": [
                  "established_by_capture_id"
                ]
              },
              {
                "required": [
                  "established_by_activity_id"
                ]
              }
            ],
            "required": [
              "state",
              "relative_payload_locator",
              "basis"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "operation": {
                "const": "unbind"
              }
            },
            "required": [
              "operation"
            ]
          },
          "then": {
            "not": {
              "anyOf": [
                {
                  "required": [
                    "state"
                  ]
                },
                {
                  "required": [
                    "relative_payload_locator"
                  ]
                },
                {
                  "required": [
                    "established_by_capture_id"
                  ]
                },
                {
                  "required": [
                    "established_by_activity_id"
                  ]
                },
                {
                  "required": [
                    "basis"
                  ]
                },
                {
                  "required": [
                    "basis_uri"
                  ]
                }
              ]
            },
            "required": [
              "replaces_binding_id"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "basis": {
                "const": "other"
              }
            },
            "required": [
              "basis"
            ]
          },
          "then": {
            "required": [
              "basis_uri"
            ]
          }
        }
      ],
      "description": "Append-only assertion binding a journal state to a payload locator relative to the sidecar. The latest effective event per role determines the current binding.",
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "basis": {
          "enum": [
            "size_and_sha256",
            "repository_assertion",
            "other"
          ],
          "type": "string"
        },
        "basis_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "established_by_activity_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "established_by_capture_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "operation": {
          "enum": [
            "bind",
            "unbind"
          ],
          "type": "string"
        },
        "relative_payload_locator": {
          "$ref": "#/$defs/locator"
        },
        "replaces_binding_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "role": {
          "enum": [
            "co_resident_primary_payload",
            "related_payload",
            "other"
          ],
          "type": "string"
        },
        "role_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "payload_binding"
        }
      },
      "required": [
        "id",
        "type",
        "operation",
        "role",
        "asserted_by_agent_id"
      ],
      "type": "object"
    },
    "personAgent": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "person"
        }
      },
      "required": [
        "id",
        "type"
      ],
      "type": "object"
    },
    "portableString": {
      "pattern": "^[^\\u0000\\uD800-\\uDFFF]*$",
      "type": "string"
    },
    "principal": {
      "additionalProperties": false,
      "anyOf": [
        {
          "required": [
            "name"
          ]
        },
        {
          "required": [
            "identifiers"
          ]
        }
      ],
      "properties": {
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "kind": {
          "enum": [
            "user",
            "group",
            "service",
            "device",
            "unknown"
          ],
          "type": "string"
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "resolution": {
          "enum": [
            "resolved",
            "unresolved",
            "not_attempted"
          ],
          "type": "string"
        }
      },
      "required": [
        "kind",
        "resolution"
      ],
      "type": "object"
    },
    "processDetail": {
      "additionalProperties": false,
      "minProperties": 1,
      "properties": {
        "command_line": {
          "items": {
            "$ref": "#/$defs/portableString"
          },
          "minItems": 1,
          "type": "array"
        },
        "configuration_digest": {
          "$ref": "#/$defs/digest"
        },
        "external_event_identifier": {
          "$ref": "#/$defs/identifier"
        },
        "plan_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "working_directory": {
          "$ref": "#/$defs/locator"
        }
      },
      "type": "object"
    },
    "processEvidence": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "basis": {
                "const": "other"
              }
            },
            "required": [
              "basis"
            ]
          },
          "then": {
            "required": [
              "basis_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "basis": {
                "const": "inferred_from_state_comparison"
              }
            },
            "required": [
              "basis"
            ]
          },
          "then": {
            "required": [
              "comparison_relation_id"
            ]
          }
        }
      ],
      "description": "Evidence supporting the asserted process type. Activity labels such as transcode are process provenance, never inferred from payload bytes unless explicitly marked as a comparison-based inference.",
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "basis": {
          "enum": [
            "direct_process_record",
            "application_log",
            "operating_system_audit",
            "repository_workflow",
            "user_attestation",
            "imported_provenance",
            "inferred_from_state_comparison",
            "unknown",
            "other"
          ],
          "type": "string"
        },
        "basis_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "comparison_relation_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "confidence": {
          "enum": [
            "high",
            "medium",
            "low",
            "unknown"
          ],
          "type": "string"
        },
        "description": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "reference": {
          "$ref": "#/$defs/typedValue"
        }
      },
      "required": [
        "id",
        "basis",
        "asserted_by_agent_id",
        "confidence"
      ],
      "type": "object"
    },
    "provenanceContractReference": {
      "additionalProperties": false,
      "properties": {
        "contract_id": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "contract_sha256": {
          "$ref": "#/$defs/sha256Hex"
        },
        "format": {
          "const": "riverhog-provenance-contract-reference/v1"
        },
        "provider": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "format",
        "provider",
        "contract_id",
        "contract_sha256"
      ],
      "type": "object"
    },
    "provenanceObserverReference": {
      "additionalProperties": false,
      "dependentRequired": {
        "distribution": [
          "version"
        ],
        "version": [
          "distribution"
        ]
      },
      "properties": {
        "contract": {
          "$ref": "#/$defs/provenanceContractReference"
        },
        "distribution": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "format": {
          "const": "riverhog-provenance-observer-reference/v1"
        },
        "observer_id": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "provider": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "format",
        "provider",
        "observer_id",
        "contract"
      ],
      "type": "object"
    },
    "relation": {
      "oneOf": [
        {
          "$ref": "#/$defs/usageRelation"
        },
        {
          "$ref": "#/$defs/generationRelation"
        },
        {
          "$ref": "#/$defs/derivationRelation"
        },
        {
          "$ref": "#/$defs/invalidationRelation"
        },
        {
          "$ref": "#/$defs/continuityRelation"
        },
        {
          "$ref": "#/$defs/comparisonRelation"
        }
      ]
    },
    "relationRole": {
      "enum": [
        "source",
        "input",
        "reference",
        "component",
        "metadata_source",
        "result",
        "output",
        "derivative",
        "replacement",
        "copy",
        "other"
      ],
      "type": "string"
    },
    "runtime": {
      "additionalProperties": false,
      "properties": {
        "character_encoding": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "container": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "effective_principal": {
          "$ref": "#/$defs/principal"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "locale": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "privilege": {
          "enum": [
            "unprivileged",
            "elevated",
            "root",
            "system",
            "unknown"
          ],
          "type": "string"
        },
        "process_architecture": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "time_zone": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "utc_offset": {
          "pattern": "^[+-]\u0028?:[01][0-9]|2[0-3]):[0-5][0-9]$",
          "type": "string"
        }
      },
      "required": [
        "process_architecture",
        "privilege"
      ],
      "type": "object"
    },
    "semanticAssertion": {
      "additionalProperties": false,
      "description": "URI-named semantic assertion over the closed typed-value union. It cannot redefine core Riverhog provenance semantics.",
      "properties": {
        "asserted_by_agent_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "confidence": {
          "enum": [
            "high",
            "medium",
            "low",
            "unknown"
          ],
          "type": "string"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "note": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "property": {
          "$ref": "#/$defs/absoluteUri"
        },
        "subject_id": {
          "$ref": "#/$defs/absoluteUri"
        },
        "type": {
          "const": "semantic_assertion"
        },
        "value": {
          "$ref": "#/$defs/typedValue"
        }
      },
      "required": [
        "id",
        "type",
        "subject_id",
        "property",
        "value",
        "asserted_by_agent_id",
        "confidence"
      ],
      "type": "object"
    },
    "sha256Hex": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    },
    "softwareAgent": {
      "additionalProperties": false,
      "anyOf": [
        {
          "required": [
            "version"
          ]
        },
        {
          "required": [
            "build"
          ]
        },
        {
          "required": [
            "executable_digests"
          ]
        }
      ],
      "properties": {
        "build": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "executable_digests": {
          "items": {
            "$ref": "#/$defs/digest"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "identifiers": {
          "items": {
            "$ref": "#/$defs/identifier"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "software"
        },
        "vendor": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "id",
        "type",
        "name"
      ],
      "type": "object"
    },
    "sourceDescriptor": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "platform": {
                "const": "other"
              }
            },
            "required": [
              "platform"
            ]
          },
          "then": {
            "required": [
              "platform_name"
            ]
          }
        }
      ],
      "properties": {
        "api": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "api_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "field": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "field_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "platform": {
          "$ref": "#/$defs/sourcePlatform"
        },
        "platform_name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "version": {
          "$ref": "#/$defs/nonEmptyString"
        }
      },
      "required": [
        "platform",
        "api"
      ],
      "type": "object"
    },
    "sourcePlatform": {
      "enum": [
        "linux",
        "windows",
        "macos",
        "freebsd",
        "openbsd",
        "netbsd",
        "illumos",
        "aix",
        "posix",
        "android",
        "ios",
        "solaris",
        "other"
      ],
      "type": "string"
    },
    "stateReference": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "scope": {
                "const": "local"
              }
            },
            "required": [
              "scope"
            ]
          },
          "then": {
            "not": {
              "anyOf": [
                {
                  "required": [
                    "journal_id"
                  ]
                },
                {
                  "required": [
                    "entry_id"
                  ]
                },
                {
                  "required": [
                    "entry_json_sha256"
                  ]
                },
                {
                  "required": [
                    "sidecar_uri"
                  ]
                }
              ]
            }
          }
        },
        {
          "if": {
            "properties": {
              "scope": {
                "const": "external"
              }
            },
            "required": [
              "scope"
            ]
          },
          "then": {
            "required": [
              "journal_id"
            ]
          }
        }
      ],
      "dependentRequired": {
        "entry_id": [
          "entry_json_sha256"
        ],
        "entry_json_sha256": [
          "entry_id"
        ]
      },
      "properties": {
        "entry_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "entry_json_sha256": {
          "$ref": "#/$defs/sha256Hex"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "journal_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "scope": {
          "enum": [
            "local",
            "external"
          ],
          "type": "string"
        },
        "sidecar_uri": {
          "$ref": "#/$defs/absoluteUri"
        }
      },
      "required": [
        "id",
        "scope"
      ],
      "type": "object"
    },
    "textValue": {
      "additionalProperties": false,
      "dependentRequired": {
        "byte_length": [
          "source_encoding"
        ]
      },
      "properties": {
        "byte_length": {
          "maximum": "9223372036854775807",
          "minimum": 0,
          "type": "integer"
        },
        "data": {
          "$ref": "#/$defs/portableString"
        },
        "language": {
          "pattern": "^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$",
          "type": "string"
        },
        "media_type": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "source_encoding": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "type": {
          "const": "text"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "timestampObservation": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "kind": {
                "const": "other"
              }
            },
            "required": [
              "kind"
            ]
          },
          "then": {
            "required": [
              "kind_uri"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "value_status": {
                "enum": [
                  "exact",
                  "assumed"
                ]
              }
            },
            "required": [
              "value_status"
            ]
          },
          "then": {
            "required": [
              "value",
              "resolution_ns"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "value_status": {
                "const": "assumed"
              }
            },
            "required": [
              "value_status"
            ]
          },
          "then": {
            "required": [
              "assumption"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "value_status": {
                "const": "unresolved"
              }
            },
            "required": [
              "value_status"
            ]
          },
          "then": {
            "not": {
              "required": [
                "value"
              ]
            },
            "required": [
              "raw_value"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "raw_unit": {
                "const": "other"
              }
            },
            "required": [
              "raw_unit"
            ]
          },
          "then": {
            "required": [
              "raw_unit_name"
            ]
          }
        }
      ],
      "dependentRequired": {
        "raw_epoch": [
          "raw_value"
        ],
        "raw_unit": [
          "raw_value"
        ]
      },
      "properties": {
        "assumption": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "kind": {
          "enum": [
            "created",
            "content_modified",
            "metadata_changed",
            "accessed",
            "backup",
            "archived",
            "other"
          ],
          "type": "string"
        },
        "kind_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "raw_epoch": {
          "$ref": "#/$defs/utcDateTime"
        },
        "raw_unit": {
          "enum": [
            "seconds",
            "milliseconds",
            "microseconds",
            "nanoseconds",
            "ticks_100ns",
            "days",
            "other"
          ],
          "type": "string"
        },
        "raw_unit_name": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "raw_value": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "resolution_ns": {
          "maximum": "9223372036854775807",
          "minimum": 1,
          "type": "integer"
        },
        "source": {
          "$ref": "#/$defs/fieldSourceDescriptor"
        },
        "value": {
          "$ref": "#/$defs/utcDateTime"
        },
        "value_status": {
          "enum": [
            "exact",
            "assumed",
            "unresolved"
          ],
          "type": "string"
        }
      },
      "required": [
        "kind",
        "value_status",
        "source"
      ],
      "type": "object"
    },
    "timestampValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/utcDateTime"
        },
        "resolution_ns": {
          "maximum": "9223372036854775807",
          "minimum": 1,
          "type": "integer"
        },
        "type": {
          "const": "timestamp"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "transitionActivity": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "event_type": {
                "const": "other"
              }
            },
            "required": [
              "event_type"
            ]
          },
          "then": {
            "required": [
              "event_type_uri"
            ]
          }
        }
      ],
      "description": "A real-world Activity/PREMIS Event that used, generated, or invalidated file states. It is distinct from observation capture.",
      "properties": {
        "associations": {
          "items": {
            "$ref": "#/$defs/association"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "detail": {
          "$ref": "#/$defs/processDetail"
        },
        "environment_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "event_label": {
          "$ref": "#/$defs/nonEmptyString"
        },
        "event_type": {
          "enum": [
            "creation",
            "content_modification",
            "metadata_update",
            "transformation",
            "copy",
            "relocation",
            "deletion",
            "composite",
            "unknown_change",
            "other"
          ],
          "type": "string"
        },
        "event_type_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "evidence": {
          "items": {
            "$ref": "#/$defs/processEvidence"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "notes": {
          "items": {
            "$ref": "#/$defs/nonEmptyString"
          },
          "minItems": 1,
          "type": "array",
          "uniqueItems": true
        },
        "outcome": {
          "enum": [
            "success",
            "partial",
            "failure",
            "unknown"
          ],
          "type": "string"
        },
        "time": {
          "$ref": "#/$defs/activityTime"
        },
        "type": {
          "const": "file_state_transition"
        }
      },
      "required": [
        "id",
        "type",
        "event_type",
        "time",
        "outcome",
        "evidence"
      ],
      "type": "object"
    },
    "typedValue": {
      "description": "Closed tagged value union.",
      "oneOf": [
        {
          "$ref": "#/$defs/capturedValue"
        },
        {
          "$ref": "#/$defs/digestOnlyValue"
        }
      ]
    },
    "uriValue": {
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/absoluteUri"
        },
        "type": {
          "const": "uri"
        }
      },
      "required": [
        "type",
        "data"
      ],
      "type": "object"
    },
    "urnUuid": {
      "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
      "type": "string"
    },
    "usageRelation": {
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "role": {
                "const": "other"
              }
            },
            "required": [
              "role"
            ]
          },
          "then": {
            "required": [
              "role_uri"
            ]
          }
        }
      ],
      "properties": {
        "activity_id": {
          "$ref": "#/$defs/urnUuid"
        },
        "id": {
          "$ref": "#/$defs/urnUuid"
        },
        "role": {
          "$ref": "#/$defs/relationRole"
        },
        "role_uri": {
          "$ref": "#/$defs/absoluteUri"
        },
        "state": {
          "$ref": "#/$defs/stateReference"
        },
        "type": {
          "const": "usage"
        }
      },
      "required": [
        "id",
        "type",
        "activity_id",
        "state",
        "role"
      ],
      "type": "object"
    },
    "utcDateTime": {
      "description": "RFC 3339 date-time normalized to UTC with uppercase Z and at most nanosecond precision.",
      "format": "date-time",
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,9})?Z$",
      "type": "string"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "allOf": [
    {
      "if": {
        "properties": {
          "entry_kind": {
            "const": "journal_init"
          }
        },
        "required": [
          "entry_kind"
        ]
      },
      "then": {
        "not": {
          "required": [
            "previous_entry"
          ]
        },
        "properties": {
          "body": {
            "$ref": "#/$defs/journalInitBody"
          },
          "sequence": {
            "const": 0
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "entry_kind": {
            "const": "assertion"
          }
        },
        "required": [
          "entry_kind"
        ]
      },
      "then": {
        "properties": {
          "body": {
            "$ref": "#/$defs/assertionBody"
          },
          "sequence": {
            "minimum": 1
          }
        },
        "required": [
          "previous_entry"
        ]
      }
    },
    {
      "if": {
        "properties": {
          "entry_kind": {
            "const": "correction"
          }
        },
        "required": [
          "entry_kind"
        ]
      },
      "then": {
        "properties": {
          "body": {
            "$ref": "#/$defs/correctionBody"
          },
          "sequence": {
            "minimum": 1
          }
        },
        "required": [
          "previous_entry"
        ]
      }
    },
    {
      "if": {
        "properties": {
          "entry_kind": {
            "const": "checkpoint"
          }
        },
        "required": [
          "entry_kind"
        ]
      },
      "then": {
        "properties": {
          "body": {
            "$ref": "#/$defs/checkpointBody"
          },
          "sequence": {
            "minimum": 1
          }
        },
        "required": [
          "previous_entry"
        ]
      }
    }
  ],
  "description": "One immutable entry in a hash-chained RFC 7464 per-file provenance journal.",
  "properties": {
    "$schema": {
      "const": "https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json"
    },
    "body": {
      "type": "object"
    },
    "entry_kind": {
      "enum": [
        "journal_init",
        "assertion",
        "correction",
        "checkpoint"
      ],
      "type": "string"
    },
    "id": {
      "$ref": "#/$defs/urnUuid"
    },
    "journal_id": {
      "$ref": "#/$defs/urnUuid"
    },
    "notes": {
      "items": {
        "$ref": "#/$defs/nonEmptyString"
      },
      "minItems": 1,
      "type": "array",
      "uniqueItems": true
    },
    "previous_entry": {
      "$ref": "#/$defs/entryReference"
    },
    "profile": {
      "const": "https://nashspence.github.io/riverhog/v1/provenance"
    },
    "recorded_at": {
      "$ref": "#/$defs/utcDateTime"
    },
    "recorded_by_agent_id": {
      "$ref": "#/$defs/urnUuid"
    },
    "recording_environment_id": {
      "$ref": "#/$defs/urnUuid"
    },
    "schema_version": {
      "const": "1.0.0"
    },
    "sequence": {
      "maximum": "9223372036854775807",
      "minimum": 0,
      "type": "integer"
    },
    "type": {
      "const": "riverhog_provenance_journal_entry"
    }
  },
  "required": [
    "$schema",
    "profile",
    "schema_version",
    "id",
    "type",
    "journal_id",
    "sequence",
    "recorded_at",
    "recorded_by_agent_id",
    "entry_kind",
    "body"
  ],
  "title": "Riverhog provenance v1 journal entry",
  "type": "object"
}
```
