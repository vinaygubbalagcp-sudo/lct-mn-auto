import re

def validate_filename(file_path):
    """
    Validates that the file name matches:
    interviews_data_YYYYMMDD_HHMMSS.csv
    """

    pattern = r"^gs:\/\/[a-zA-Z0-9\-]+\/interviews_scheduled_folder\/raw\/interviews_data_\d{8}_\d{6}\.csv$"

    if re.match(pattern, file_path):
        return True
    else:
        return False


# ---------- Example Usage ----------
file_path = "gs://lvc-tc-mn-d-bckt/interviews_scheduled_folder/raw/interviews_data_20251209_000015.csv"

if validate_filename(file_path):
    print("VALID filename")
else:
    print("INVALID filename")
