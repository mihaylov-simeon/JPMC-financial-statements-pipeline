from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("Financial Statements Analysis - JPMorgan").master("local[*]").getOrCreate()

# Read gold layer data and convert to Pandas for plotting
df = spark.read.parquet("data/gold/financial_statements_jpm_gold")
pdf = df.toPandas()

# Dark theme settings
plt.rcParams.update({
    "figure.facecolor": "#0e1117",
    "axes.facecolor": "#0e1117",
    "axes.edgecolor": "#aaaaaa",
    "axes.labelcolor": "#ffffff",
    "text.color": "#ffffff",
    "xtick.color": "#ffffff",
    "ytick.color": "#ffffff",
    "grid.color": "#5E5E5E",
    "font.size": 10,
})

# Filter for Total Assets and sort by date
assets = pdf[pdf["CATEGORY"] == "Total Assets"].sort_values(by="TRX_DT")
# Convert None -> NaN
assets['YOY_PCT_CHG'] = assets["YOY_PCT_CHG"].astype(float)
# Calculate YoY percentage change and acceleration
assets["YOY_ACCELERATION"] = assets["YOY_PCT_CHG"].diff()
# Create first figure
fig, ax1 = plt.subplots()

# Set the first axis for the YoY % Change
ax1.plot(assets["TRX_DT"], assets["YOY_PCT_CHG"], marker='o', label="YoY %")
ax1.set_xlabel("Transaction Date")
ax1.set_ylabel("YoY Percentage Change")
ax1.yaxis.set_major_formatter(lambda x, _: f"{x:.1f}%")

# Grid design for ax1
ax1.grid(which='major', linestyle='--', alpha=0.7)
ax1.grid(which='minor', linestyle=':', alpha=0.5)
ax1.set_title("JPMorgan Total Assets: YoY % Change and Acceleration Over Time")
ax1.minorticks_on()

# Create second y-axis for YoY Acceleration
ax2 = ax1.twinx()
ax2.plot(assets["TRX_DT"], assets["YOY_ACCELERATION"], marker='s', color='orange', label='YoY Acceleration')
ax2.set_ylabel("YoY Acceleration (percentage points)")

# Grid design for ax2 (mostly will inherit from ax1)
ax2.minorticks_on()

# Readability improvements
fig.autofmt_xdate(rotation=15)
fig.tight_layout()

ax1.legend(loc="upper left")
ax2.legend(loc="upper right")

# Annotation on YoY Acceleration final point
ax2.annotate(
    "Growth decelerates",
    xy=(assets["TRX_DT"].iloc[-1], assets["YOY_ACCELERATION"].iloc[-1]),
    xytext=(0, -60),
    textcoords="offset points",
    arrowprops=dict(arrowstyle="->", color="orange"),
    color="orange",
    ha="center"
)

plt.show()