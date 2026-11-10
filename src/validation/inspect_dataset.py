from pathlib import Path
import pandas as pd

project_root = Path(__file__).resolve().parents[2]
raw_folder = project_root / "data" / "raw"


csv_file = raw_folder / "ncr_ride_bookings.csv"

if not csv_file.exists():
    raise FileNotFoundError(f"CSV file not found: {csv_file}")

df = pd.read_csv(csv_file)

# Display the results
print("Dataset:", csv_file.name)

print("\nFirst five rows:")
print(df)

print("\nColumn names:")
print(df.columns.tolist())

print("\nData types:")
print(df.dtypes)


#full data shape

print("\nTotal rows and columns:")
print(df.shape)

#missing values
print("\nMissing values by column:")
print(df.isnull().sum())

#percentage of missing values
print("\nMissing value percentage:")
print((df.isnull().mean() * 100).round(2))

#duplicate rows
print("\nDuplicate rows:")
print(df.duplicated().sum())

# Booking status distribution
print("\n--- Booking Status Distribution ---")
print(df["Booking Status"].value_counts(dropna=False))


# Missing values by booking status (all 21 columns)
print("\n--- Missing Values by Booking Status ---")

missing_by_status = (
    df.groupby("Booking Status")
    .apply(lambda group: group.isnull().sum())
)

print(missing_by_status)


# Check negative values in all numeric columns
print("\n--- Negative Value Checks ---")

numeric_columns = df.select_dtypes(include="number").columns

for column in numeric_columns:
    negative_count = (df[column] < 0).sum()
    print(f"{column}: {negative_count}")


# 4. Check rating ranges
print("\n--- Rating Range Checks ---")

for column in ["Driver Ratings", "Customer Rating"]:
    print(f"\n{column}")
    print("Minimum:", df[column].min())
    print("Maximum:", df[column].max())


# 5. Check unique values in categorical columns
print("\n--- Booking Status Values ---")
print(df["Booking Status"].unique())

print("\n--- Vehicle Types ---")
print(df["Vehicle Type"].unique())

print("\n--- Payment Methods ---")
print(df["Payment Method"].unique())