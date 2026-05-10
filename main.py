"""Interactive VSCode Notebook"""
# %%
# Submit batch job: SPY option chain for the last 10 trading days

import os
import databento as db
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()
client = db.Historical(os.environ["DATABENTO_API_KEY"])

end_date = date.today()
start_date = end_date - timedelta(days=14)  # ~14 calendar days to cover 10 trading days

details = client.batch.submit_job(
    dataset="OPRA.PILLAR",
    symbols=["SPY.OPT"],
    stype_in="parent",
    schema="ohlcv-1d",
    encoding="dbn",
    start=start_date.isoformat(),
    end=end_date.isoformat(),
)
print(details)

# %%
# Poll until the job is done, then download

import time

job_id = details["id"]
while True:
    jobs = client.batch.list_jobs(
        states=["queued", "processing", "done"],
        since="2026-05-05",
    )
    job = [job for job in jobs if job["id"] == job_id][0]
    print(f"Status: {job['state']}")
    if job["state"] in ("done", "expired", "failed"):
        break
    time.sleep(5)

if job["state"] == "done":
    files = client.batch.download(job_id, output_dir="./spy_optchain")
    print(f"Downloaded {len(files)} file(s): {files}")
else:
    print(f"Job ended with state: {job.state}")


# %%
client = db.Historical(os.environ["DATABENTO_API_KEY"])

available_range = client.metadata.get_dataset_range(dataset="OPRA.PILLAR")
available_range["start"]
print(available_range)
# %%
import databento as db
from pathlib import Path

files = sorted(Path("data/spy_optchain/files").glob("*.dbn.zst"))
store = db.DBNStore.from_file(str(files[0]))
df = store.to_df(map_symbols=True)
df

# %%
