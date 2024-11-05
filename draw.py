import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import os
import csv
import logging as log

# Load data
file_size_mb = 64  # File size in MB (as used in the Go code)
numThreads = 1  # File size in MB (as used in the Go code)
dir_local_eu = "Local-EU"
dir_eu_eu = "EU-EU"
dir_us_eu = "US-EU"
df = pd.read_csv(f"data/{dir_us_eu}/upload_times_{file_size_mb}MB_{numThreads}Thr.csv")

# Add upload speed column
df["Upload Speed (MB/s)"] = file_size_mb / df["Upload Time (seconds)"]

# Use TkAgg for Matplotlib if necessary
matplotlib.use('TkAgg')

# Plot both distributions in one figure with two subplots (one above the other)
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

# Plot histogram of upload times
ax1.hist(df["Upload Time (seconds)"], bins=10, edgecolor='black', alpha=0.6)
ax1.set_title("Distribution of Upload Times for 8 MB Files")
ax1.set_xlabel("Upload Time (seconds)")
ax1.set_ylabel("Frequency")

# Plot histogram of upload speeds
ax2.hist(df["Upload Speed (MB/s)"], bins=10, edgecolor='black', alpha=0.6, color='skyblue')
ax2.set_title("Distribution of Upload Speeds for 8 MB Files")
ax2.set_xlabel("Upload Speed (MB/s)")
ax2.set_ylabel("Frequency")

plt.tight_layout()

# Compute mean and standard deviation of upload times
mean_upload_time = df["Upload Time (seconds)"].mean()
std_dev_upload_time = df["Upload Time (seconds)"].std()

print(f"Mean Upload Time: {mean_upload_time:.2f} seconds")
print(f"Standard Deviation of Upload Time: {std_dev_upload_time:.2f} seconds")

# Compute mean and standard deviation of upload speeds
mean_upload_speed = df["Upload Speed (MB/s)"].mean()
std_dev_upload_speed = df["Upload Speed (MB/s)"].std()

print(f"Mean Upload Speed: {mean_upload_speed:.2f} MB/s")
print(f"Standard Deviation of Upload Speed: {std_dev_upload_speed:.2f} MB/s")

plt.show()