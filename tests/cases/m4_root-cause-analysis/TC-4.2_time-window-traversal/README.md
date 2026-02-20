# TC-4.2: Time Window Filtering in Traversal

Goal: Traverse only edges within a time window.
Given: Causal edges spanning 10 years.
When: Traversing with 5-year window.
Then: Only edges within window are followed.

Preconditions:
- Edge timestamps stored.

Steps:
1. Create causal edges with timestamps in 2014, 2018, 2023.
2. Traverse backward with window 2017-2022.

Expected:
- Only edges within 2017-2022 are followed.
