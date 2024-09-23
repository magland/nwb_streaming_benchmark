import time
from pynwb import NWBHDF5IO
from dandi.dandiapi import DandiAPIClient
from fsspec import filesystem
from h5py import File as H5pyFile
import remfile
import lindi


def stream_nwbfile_lindi_precomputed(nwb_url: str):
    file = lindi.LindiH5pyFile.from_lindi_file(nwb_url)
    io = NWBHDF5IO(file=file, load_namespaces=True)
    nwbfile = io.read()
    return nwbfile, io


def stream_nwbfile_remfile(nwb_url: str):
    file = remfile.File(nwb_url)
    h5f = H5pyFile(file, mode="r")
    io = NWBHDF5IO(file=h5f, load_namespaces=True)
    nwbfile = io.read()
    return nwbfile, io


def stream_nwbfile_lindi(nwb_url: str):
    file = lindi.LindiH5pyFile.from_hdf5_file(nwb_url)
    io = NWBHDF5IO(file=file, load_namespaces=True)
    nwbfile = io.read()
    return nwbfile, io


def stream_nwbfile_fsspec(nwb_url: str):
    fs = filesystem("http")
    file_system = fs.open(nwb_url, "rb")
    file = H5pyFile(file_system, mode="r")
    io = NWBHDF5IO(file=file, load_namespaces=True)
    nwbfile = io.read()
    return nwbfile, io


# https://neurosift.app/?p=/nwb&url=https://api.dandiarchive.org/api/assets/db2372af-f041-42c8-a5f1-594be5a83c9e/download/&dandisetId=000458&dandisetVersion=draft
DANDISET_ID = "000458"
file_path = "sub-586468/sub-586468_ses-20210819_behavior+ecephys.nwb"

with DandiAPIClient() as client:
    asset = client.get_dandiset(DANDISET_ID, 'draft').get_asset_by_path(file_path)
    asset_url = asset.get_content_url(follow_redirects=0, strip_query=True)
    asset_id = asset_url.split("/")[5]
    lindi_precomputed_url = f'https://lindi.neurosift.org/dandi/dandisets/{DANDISET_ID}/assets/{asset_id}/nwb.lindi.json'

num_trials = 5

elapsed_times_lindi_precomputed = []
elapsed_times_remfile = []
elapsed_times_lindi = []
elapsed_times_fsspec = []

for trial_num in range(1, num_trials + 1):
    print(f"Trial {trial_num} of {num_trials}")

    timer = time.time()
    nwbfile, io = stream_nwbfile_lindi_precomputed(lindi_precomputed_url)
    io.close()
    elapsed = time.time() - timer
    print(f"Elapsed time for lindi precomputed: {elapsed} s")
    elapsed_times_lindi_precomputed.append(elapsed)

    timer = time.time()
    nwbfile, io = stream_nwbfile_remfile(asset_url)
    io.close()
    elapsed = time.time() - timer
    print(f"Elapsed time for remfile: {elapsed} s")
    elapsed_times_remfile.append(elapsed)

    timer = time.time()
    nwbfile, io = stream_nwbfile_lindi(asset_url)
    io.close()
    elapsed = time.time() - timer
    print(f"Elapsed time for lindi: {elapsed} s")
    elapsed_times_lindi.append(elapsed)

    timer = time.time()
    nwbfile, io = stream_nwbfile_fsspec(asset_url)
    io.close()
    elapsed = time.time() - timer
    print(f"Elapsed time for fsspec: {elapsed} s")

    print('')

average_elapsed_time_lindi_precomputed = sum(elapsed_times_lindi_precomputed) / num_trials
average_elapsed_time_remfile = sum(elapsed_times_remfile) / num_trials
average_elapsed_time_lindi = sum(elapsed_times_lindi) / num_trials
average_elapsed_time_fsspec = sum(elapsed_times_fsspec) / num_trials

print(f"Average elapsed time for lindi precomputed: {average_elapsed_time_lindi_precomputed} s")
print(f"Average elapsed time for remfile: {average_elapsed_time_remfile} s")
print(f"Average elapsed time for lindi: {average_elapsed_time_lindi} s")
print(f"Average elapsed time for fsspec: {average_elapsed_time_fsspec} s")
