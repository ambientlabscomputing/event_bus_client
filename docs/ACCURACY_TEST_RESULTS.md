# Target Field Filtering Accuracy Test Results

## Test Overview
**Test Name:** accuracy_test_target_filtering  
**Duration:** 1 minute  
**Objective:** Verify that target field filtering correctly routes messages to specific subscribers

---

## Test Configuration

### Publishers (480 total messages @ 8 msg/s):
1. **publish_user_123_messages** - 120 msgs with `target_type=user`, `target_id=user-123`
2. **publish_org_456_messages** - 120 msgs with `target_type=organization`, `target_id=org-456`
3. **publish_user_789_messages** - 120 msgs with `target_type=user`, `target_id=user-789`
4. **publish_no_target_messages** - 120 msgs with no target fields

### Subscribers (each with unique group ID for isolation):
1. **subscribe_user_type_only** - Filter: `target_type=user`
2. **subscribe_org_type_only** - Filter: `target_type=organization`
3. **subscribe_user_123_exact** - Filter: `target_type=user` AND `target_id=user-123`
4. **subscribe_user_789_exact** - Filter: `target_type=user` AND `target_id=user-789`
5. **subscribe_all_no_filter** - No filters (receives all messages)

---

## Actual Results

```
Messages Published: 480
Messages Received:  2,380 (across 5 subscribers)

Each subscriber received: 476 messages
```

---

## ⚠️ CRITICAL FINDING: Target Field Filtering Not Working

### Expected Behavior:
- **subscribe_user_type_only** → Should receive 240 msgs (user-123 + user-789)
- **subscribe_org_type_only** → Should receive 120 msgs (org-456 only)
- **subscribe_user_123_exact** → Should receive 120 msgs (user-123 only)
- **subscribe_user_789_exact** → Should receive 120 msgs (user-789 only)
- **subscribe_all_no_filter** → Should receive 480 msgs (all messages)

### Actual Behavior:
**All subscribers received 476 messages regardless of filters** 🚨

This indicates:
1. Target field filters are **NOT being applied** on the backend
2. All subscribers are receiving the same message stream
3. The filter parameters are being passed correctly from the client (verified in CLI args)

---

## Analysis

### Message Math:
- Published during test: 480 messages
- Each subscriber received: 476 messages
- Missing: 4 messages per subscriber

The 4-message discrepancy is likely due to:
- Test ramp-up timing (subscribers connect slightly after publishers start)
- Or catching up on recent backlog from topic initialization

### Filter Pass-Through Verified:
The load test framework correctly passes `--target-type` and `--target-id` flags to the CLI, and the CLI accepts them. However, the backend is not filtering based on these parameters.

---

## Recommendations for Backend Team

### P0 - Critical Bug:
**Target field subscription filtering is not implemented or broken**

The backend must:
1. Parse `target_type` and `target_id` from subscription request
2. Store these filters with the consumer group metadata
3. Apply filters during message fetch/delivery
4. Only return messages matching the subscriber's target criteria

### Expected Filter Logic:
```
If subscriber has target_type=X:
  - Return messages where target_type=X
  - Include messages with no target_type (broadcast)

If subscriber has target_type=X AND target_id=Y:
  - Return messages where target_type=X AND target_id=Y
  - Include messages with target_type=X but no target_id
  - Include messages with no target fields (broadcast)

If subscriber has no filters:
  - Return all messages
```

### Testing Steps to Verify Fix:
1. Subscribe with `--target-type user --target-id user-123`
2. Publish messages with various target combinations
3. Verify subscriber only receives matching messages
4. Re-run this accuracy test - all subscriber counts should differ based on filters

---

## Next Steps

1. ✅ Client-side implementation complete (CLI accepts and passes filters)
2. ⏳ **BLOCKED:** Waiting for backend target field filtering implementation
3. ⏳ After backend fix: Re-run accuracy test to verify correct behavior
4. ⏳ Add more edge cases (multiple target types, null handling, etc.)

---

## Test Configuration Reference

Full test config available at: `loadtest/examples/accuracy.json`

Key features:
- Low volume (8 msg/s total) for accuracy verification
- Each subscriber gets unique group ID for complete isolation
- Multiple filter combinations to test different scenarios
- Includes baseline "no filter" subscriber for comparison
