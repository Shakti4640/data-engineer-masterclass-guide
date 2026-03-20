# --- EXECUTION ORDER ---

# 1. Create IAM groups (one-time)
python 10_create_iam_groups.py

# 2. Create and attach IAM policies (one-time)
python 11_create_iam_policies.py

# 3. Apply bucket policy DENY rules (one-time)
python 12_create_bucket_policy.py

# 4. Create test users (testing only)
python 13_create_test_users.py

# 5. Test access controls
python 14_test_access_controls.py

# 6. Visualize access matrix (documentation)
python 16_visualize_access_matrix.py

# 7. Cleanup test users when done
python 15_cleanup_access_controls.py