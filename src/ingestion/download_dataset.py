import kagglehub
import shutil
from pathlib import Path

# Download the dataset from Kaggle
path = kagglehub.dataset_download("anilrohan/uber-data-india")

print("Dataset downloaded to:", path)

# Our project's raw data folder
project_root = Path(__file__).resolve().parents[2]
raw_folder = project_root / "data" / "raw"

raw_folder.mkdir(parents=True, exist_ok=True)

# Copy the downloaded dataset files into data/raw
for item in Path(path).iterdir():
    destination = raw_folder / item.name

    if item.is_file():
        shutil.copy2(item, destination)

print("Dataset copied to:", raw_folder)