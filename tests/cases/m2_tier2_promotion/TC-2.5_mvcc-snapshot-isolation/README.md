# TC-2.5: MVCC Snapshot Isolation

Goal: Verify readers see a consistent snapshot during fusion.
Given: Readers access Tier 2 while fusion runs.
When: Fusion swaps to a new epoch.
Then: Readers see a consistent snapshot with no partial state.

Preconditions:
- MVCC enabled for Tier 2.

Steps:
1. Start long-running read transaction on Tier 2.
2. Trigger fusion that creates a new epoch.
3. Continue reading within the transaction.

Expected:
- Reader sees only old epoch data until transaction ends.
- No partial or mixed epochs are visible.
- New epoch visible after reader refreshes.
