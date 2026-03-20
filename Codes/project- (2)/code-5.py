# --- EXECUTION ORDER ---

# 1. Enable versioning (one-time)
python 06_enable_versioning.py

# 2. Configure lifecycle rules (one-time)
python 07_configure_lifecycle.py

# 3. Run disaster simulation drill
python 08_simulate_delete_and_recover.py

# 4. (Optional) Clean up test versions
python 09_permanent_delete_version.py