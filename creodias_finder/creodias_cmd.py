import os
import zipfile
from datetime import datetime
from pathlib import Path
from time import sleep

import dropbox
import geojson
from shapely.geometry import Point
from tqdm import tqdm

from creodias_finder import query
from creodias_finder import download

DROPBOX_ACCESS_TOKEN = 'oKFU0w6IHvEAAAAAAAFxV-NhqpjyV4fRJGwBovhIN4kSDj0Ko8YqTDWA71Omrh8D'
DROPBOX_DIR = '/Apps/mysentinel2'

LOCAL_OUTPUT_DIR = 'C:\\Users\\Admin\\Sentinel-2'

MAX_CLOUD = 70.0

TILES = [
	{"name": "34UCU", "centroid_lon": 19.4, "centroid_lat": 48.0},
	{"name": "34UCV", "centroid_lon": 19.4, "centroid_lat": 49.2},
	{"name": "33UXP", "centroid_lon": 17.0, "centroid_lat": 48.0},
	{"name": "33UYP", "centroid_lon": 18.3, "centroid_lat": 48.3},
	{"name": "33UYQ", "centroid_lon": 18.3, "centroid_lat": 49.2},
]

def check_zip_file(zip_filepath):
	file_without_ext = zip_filepath.stem
	try:
		with zipfile.ZipFile(str(zip_filepath)) as zip_file:
			for member in zip_file.namelist():
				if file_without_ext in member:
					return True
				else:
					return False
	except:
		print('bad zip file {}'.format(str(zip_filepath)))
		return false



def upload(
    access_token,
    file_path,
    target_path,
    timeout=900,
    chunk_size=4 * 1024 * 1024,
):
    dbx = dropbox.Dropbox(access_token, timeout=timeout)
    with open(file_path, "rb") as f:
        file_size = os.path.getsize(file_path)
        chunk_size = 4 * 1024 * 1024
        if file_size <= chunk_size:
            print(dbx.files_upload(f.read(), target_path))
        else:
            with tqdm(total=file_size, desc="Uploaded") as pbar:
                upload_session_start_result = dbx.files_upload_session_start(
                    f.read(chunk_size)
                )
                pbar.update(chunk_size)
                cursor = dropbox.files.UploadSessionCursor(
                    session_id=upload_session_start_result.session_id,
                    offset=f.tell(),
                )
                commit = dropbox.files.CommitInfo(path=target_path)
                while f.tell() < file_size:
                    if (file_size - f.tell()) <= chunk_size:
                        print(
                            dbx.files_upload_session_finish(
                                f.read(chunk_size), cursor, commit
                            )
                        )
                    else:
                        dbx.files_upload_session_append_v2(
                            f.read(chunk_size), cursor)
                        cursor.offset = f.tell()
                    pbar.update(chunk_size)


def search_scenes(start_date, end_date, geometry, max_cloud):
	results = query.query(
	    'Sentinel2',
	    start_date=datetime(2016, 1, 1),
	    end_date=datetime(2016, 12, 31),
	    geometry=Point(LON, LAT),
	)
	filtered_results = []
	for r in results.values():
		title = r["properties"]["title"]
		if title.startswith("S2A_MSIL1C_") or title.startswith("S2A_MSIL1C"):
			print((title, r["properties"]["cloudCover"]), flush=True)
			if r["properties"]["cloudCover"] <= max_cloud:
				filtered_results.append(r)
	return sorted(filtered_results, key=lambda k: k['properties']['title'])


def download_scene(creodias_scene_uuid, out_file, credentials)
	downloaded = False
	while not downloaded:
		# download single product by product ID
		try:
			download.download(r["id"], outfile=out_file, **credentials)
			if out_file.stat().st_size < 100000:
				out_file.unlink()
				print('download incomplete..', flush=True)
				raise Exception
			#elif check_zip_file(out_file) == False:
			#	print('zip file is corrupt.')
			#	out_file.unlink()
			#	raise Exception
			downloaded = True
			print("{} downloaded successfully.".format(out_file.name), flush=True)
		except Exception as e:
			sleep(10)
			print('download exception {}, retrying ..'.format(str(e)), flush=True)
			downloaded = False


def download_scenes_for_tile(tile_centroid_lon, tile_centroid_lat):

	print("creodias_cmd.py", flush=True)

	CREDENTIALS = {
	    'username': 'jiri.kadlec@gisat.cz',
	    'password': 'Earth6378'
	}

	results = search_scenes(
		start_date=datetime(2016, 1, 1),
		end_date = datetime(2016, 12, 31),
		geometry = Point(tile_centroid_lon, tile_centroid_lat),
		max_cloud = MAX_CLOUD
	)

	for r in results:
		safe_package_name = r["properties"]["title"]
		tile_id = safe_package_name.split("_")[-2][1:6]
		creodias_scene_uuid = r["id"]
		print(tile_id, flush=True)
		print((safe_package_name, creodias_scene_uuid), flush=True)
		out_file = Path(LOCAL_OUTPUT_DIR).joinpath(tile_id, safe_package_name + ".zip")
		if not out_file.parent.is_dir():
			out_file.parent.mkdir(parents=True, exist_ok=True)
			print("created output directory: {}".format(out_file.parent))
		if out_file.is_file():
			continue

		# Downloads the .SAFE.zip from CreoDias
		download_scene(creodias_scene_uuid, out_file, CREDENTIALS)
		# Uploads the downloaded .SAFE.zip to DropBox
		dropbox_path = '/'.join(DROPBOX_DIR, tile_id, out_file.name)
		upload(DROPBOX_ACCESS_TOKEN, out_file, dropbox_path)


def main():
	for tile in TILES:
		download_scenes_for_tile(tile['centroid_lon'], tile['centroid_lat'])


if __name__ == "__main__":
	main()
