@acceptance @api @mvp @xfail_contract
Feature: Plan and images API
  The API reports the current best image plan, exposes image summaries, and registers physical copies.

  Background:
    Given an archive with planner fixtures
    And the planner has at least one candidate image

  Scenario: Read the current plan
    When the client gets "/v1/plan"
    Then the response status is 200
    And the response contains "ready", "target_bytes", "min_fill_bytes", "images", and "unplanned_bytes"
    And each plan image contains "id", "bytes", "fill", "files", "collections", and "iso_ready"
    And plan images do not contain field "volume_id"
    And each image fill equals image bytes divided by target bytes
    And images are returned in best-first order

  Scenario: Read one image summary
    Given image "img_2026-04-20_01" exists
    When the client gets "/v1/images/img_2026-04-20_01"
    Then the response status is 200
    And the response contains image id "img_2026-04-20_01"
    And the response contains "volume_id", "bytes", "fill", "files", "collections", and "iso_ready"
    And the response field "volume_id" is null

  Scenario: Explicitly finalizing an image stores volume_id and removes it from the plan
    Given image "img_2026-04-20_01" exists
    When the client gets "/v1/images/img_2026-04-20_01"
    Then the response status is 200
    And the response field "volume_id" is null
    When the client posts to "/v1/images/img_2026-04-20_01/finalize"
    Then the response status is 200
    And the response contains image id "img_2026-04-20_01"
    And the response field "volume_id" matches compact UTC timestamp
    When the client gets "/v1/plan"
    Then the response status is 200
    And the response images do not contain image id "img_2026-04-20_01"
    When the client gets "/v1/images/img_2026-04-20_01"
    Then the response status is 200
    And the response field "volume_id" matches compact UTC timestamp

  Scenario: Repeating image finalization reuses the stored volume_id
    Given image "img_2026-04-20_01" exists
    When the client posts to "/v1/images/img_2026-04-20_01/finalize"
    And the client posts to "/v1/images/img_2026-04-20_01/finalize" again
    Then the response status is 200 both times
    And both responses contain the same value for field "volume_id"

  Scenario: Downloading an ISO for a provisional image fails
    Given image "img_2026-04-20_01" exists
    When the client gets "/v1/images/img_2026-04-20_01/iso"
    Then the response status is 409
    And the error code is "invalid_state"

  Scenario: Download an ISO for a finalized image
    Given image "img_2026-04-20_01" is finalized
    When the client gets "/v1/images/img_2026-04-20_01/iso"
    Then the response status is 200
    And the response body is binary ISO content

  Rule: Downloaded ISOs match the published disc contracts
    Scenario: A ready image uses the canonical disc layout and metadata contracts
      Given image "img_2026-04-20_01" is finalized
      When the client downloads and inspects ISO for image "img_2026-04-20_01"
      Then the response status is 200
      And the downloaded ISO passes xorriso verification
      And the extracted ISO root matches the disc layout contract
      And the decrypted disc manifest matches the disc manifest contract
      And every referenced collection manifest matches the collection hash manifest contract
      And every referenced file sidecar matches the file sidecar contract
      And the current ISO README documents split-file recovery
      And the current ISO payload for "docs/tax/2022/invoice-123.pdf" decrypts to the original plaintext

    Scenario: Split image parts are listed per disc and reconstruct the logical file
      Given an archive with split planner fixtures
      And image "img_2026-04-20_03" is finalized
      And image "img_2026-04-20_04" is finalized
      When the client downloads and inspects ISO for image "img_2026-04-20_03"
      Then the response status is 200
      And the downloaded ISO passes xorriso verification
      And the extracted ISO root matches the disc layout contract
      And the decrypted disc manifest matches the disc manifest contract
      And every referenced collection manifest matches the collection hash manifest contract
      And every referenced file sidecar matches the file sidecar contract
      And the current ISO lists split file "/tax/2022/invoice-123.pdf" part 1 of 2
      And the current split payload for "/tax/2022/invoice-123.pdf" is recorded
      When the client downloads and inspects ISO for image "img_2026-04-20_04"
      Then the response status is 200
      And the downloaded ISO passes xorriso verification
      And the extracted ISO root matches the disc layout contract
      And the decrypted disc manifest matches the disc manifest contract
      And every referenced collection manifest matches the collection hash manifest contract
      And every referenced file sidecar matches the file sidecar contract
      And the current ISO lists split file "/tax/2022/invoice-123.pdf" part 2 of 2
      And the current split payload for "/tax/2022/invoice-123.pdf" is recorded
      And the recorded split payloads for "docs/tax/2022/invoice-123.pdf" reconstruct the original plaintext

  Rule: Registering a copy increases archived coverage
    Background:
      Given image "img_2026-04-20_01" covers bytes from collection "docs"
      And image "img_2026-04-20_01" is finalized

    Scenario: Register a physical copy
      When the client posts to "/v1/images/img_2026-04-20_01/copies" with id "BR-021-A" and location "Shelf B1"
      Then the response status is 200
      And the response contains copy id "BR-021-A"
      And the response contains image id "img_2026-04-20_01"
      And the response copy contains "volume_id", "location", and "created_at"
      And collection "docs" archived_bytes increases
      And collection "docs" pending_bytes decreases

    Scenario: Reusing a copy id for the same finalized image fails
      Given copy "BR-021-A" already exists
      When the client posts to "/v1/images/img_2026-04-20_01/copies" with id "BR-021-A" and location "Shelf B2"
      Then the response status is 409
      And the error code is "conflict"
