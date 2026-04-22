@acceptance @api @mvp @xfail_contract
Feature: Collections API
  The API closes staged directories into immutable logical collections.

  Rule: Closing a staged directory creates one hot collection
    Background:
      Given an empty archive
      And a staged directory "photos-2024" with deterministic fixture contents

    Scenario: Close a staged collection
      When the client posts to "/v1/collections/close" with path "/staging/photos-2024"
      Then the response status is 200
      And the response contains collection id "photos-2024"
      And the response contains the correct file count
      And the response contains the correct total bytes
      And collection "photos-2024" has hot_bytes equal to bytes
      And collection "photos-2024" has archived_bytes equal to 0
      And collection "photos-2024" has pending_bytes equal to bytes
      And collection "photos-2024" is eligible for planning

    Scenario: Re-closing the same staged path fails
      Given the staged directory "/staging/photos-2024" was already closed
      When the client posts to "/v1/collections/close" with path "/staging/photos-2024"
      Then the response status is 409
      And the error code is "conflict"

    Scenario: Close a staged collection with a slash-bearing id
      Given a staged directory "photos/2024" with deterministic fixture contents
      When the client posts to "/v1/collections/close" with path "/staging/photos/2024"
      Then the response status is 200
      And the response contains collection id "photos/2024"
      And the response contains the correct file count
      And the response contains the correct total bytes

    Scenario: Closing a descendant collection id after its ancestor fails
      Given a staged directory "photos" with deterministic fixture contents
      And the staged directory "/staging/photos" was already closed
      And a staged directory "photos/2024" with deterministic fixture contents
      When the client posts to "/v1/collections/close" with path "/staging/photos/2024"
      Then the response status is 409
      And the error code is "conflict"

    Scenario: Closing an ancestor collection id after its descendant fails
      Given a staged directory "photos/2024" with deterministic fixture contents
      And the staged directory "/staging/photos/2024" was already closed
      And a staged directory "photos" with deterministic fixture contents
      When the client posts to "/v1/collections/close" with path "/staging/photos"
      Then the response status is 409
      And the error code is "conflict"

  Rule: Collection summaries expose stable coverage fields
    Background:
      Given an archive containing collection "photos-2024"

    Scenario: Read a collection summary
      When the client gets "/v1/collections/photos-2024"
      Then the response status is 200
      And the response contains "id", "files", "bytes", "hot_bytes", "archived_bytes", and "pending_bytes"
      And pending_bytes equals bytes minus archived_bytes
      And hot_bytes is between 0 and bytes
      And archived_bytes is between 0 and bytes

    Scenario: Read a slash-bearing collection summary
      Given an archive containing collection "photos/2024"
      When the client gets "/v1/collections/photos/2024"
      Then the response status is 200
      And the response contains "id", "files", "bytes", "hot_bytes", "archived_bytes", and "pending_bytes"
      And pending_bytes equals bytes minus archived_bytes
      And hot_bytes is between 0 and bytes
      And archived_bytes is between 0 and bytes

    Scenario: Unknown collection returns not found
      When the client gets "/v1/collections/missing"
      Then the response status is 404
      And the error code is "not_found"
