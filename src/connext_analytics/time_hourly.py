from datetime import datetime

# Define the start date
start_date = datetime(2022, 12, 22, 0, 0)

# Get the current datetime
current_date = datetime.now()

# Calculate the difference in hours
difference = current_date - start_date
hours = difference.total_seconds() / 3600

print(f"Hours from December 21, 2022, to now: {hours*4:.2f}")
