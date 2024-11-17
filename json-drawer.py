import json
import plotly.express as px
import pandas as pd

# Load the JSON data
with open('data/onServer/chunked_upload_results.json', 'r') as f:
    data = json.load(f)

# Prepare data for chunk speeds and time periods
speed_data = []
time_data = []

for entry in data:
    chunk_size_mb = entry['chunk_size_mb']
    speeds = entry['chunk_speeds_mb_per_s']
    periods = entry['time_periods_seconds']

    # Add chunk speeds to the speed_data list
    speed_data.extend([{'Chunk Size (MB)': chunk_size_mb, 'Chunk Speed (MB/s)': speed} for speed in speeds])

    # Add time periods to the time_data list
    time_data.extend([{'Chunk Size (MB)': chunk_size_mb, 'Time Period (s)': period} for period in periods])

# Create dataframes for plotting
speed_df = pd.DataFrame(speed_data)
time_df = pd.DataFrame(time_data)

# Calculate average value and standard deviation for chunk speeds
speed_stats = speed_df.groupby('Chunk Size (MB)')['Chunk Speed (MB/s)'].agg(['mean', 'std']).reset_index()
print("Chunk Speeds Statistics (MB/s):")
print(speed_stats)

# Calculate average value and standard deviation for time periods
time_stats = time_df.groupby('Chunk Size (MB)')['Time Period (s)'].agg(['mean', 'std']).reset_index()
print("\nTime Periods Statistics (s):")
print(time_stats)

# Plot chunk speeds distribution with overlapping bars
speed_fig = px.histogram(
    speed_df,
    x='Chunk Speed (MB/s)',
    color='Chunk Size (MB)',
    barmode='overlay',  # Overlapping bars
    marginal='box',  # Adds boxplot above the histogram
    nbins=50,
    title="Overlapping Distribution of Chunk Speeds (MB/s)",
)
speed_fig.update_layout(
    xaxis_title="Chunk Speed (MB/s)",
    yaxis_title="Frequency",
    legend_title="Chunk Size (MB)",
    bargap=0.1  # Adjust the gap between bars for better overlap
)

# Plot time periods distribution with overlapping bars
time_fig = px.histogram(
    time_df,
    x='Time Period (s)',
    color='Chunk Size (MB)',
    barmode='overlay',  # Overlapping bars
    marginal='box',  # Adds boxplot above the histogram
    nbins=50,
    title="Overlapping Distribution of Time Periods (s)",
)
time_fig.update_layout(
    xaxis_title="Time Period (s)",
    yaxis_title="Frequency",
    legend_title="Chunk Size (MB)",
    bargap=0.1  # Adjust the gap between bars for better overlap
)

# Show the plots
speed_fig.show()
time_fig.show()
