import os
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
OUTPUT_DIR = 'C:\\Users\\Admin\\Sentinel-2'
MAX_CLOUD = 70.0
LON = 20.0
LAT = 49.0


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


def main():

	print("creodias_cmd.py", flush=True)

	CREDENTIALS = {
	    'username': 'jiri.kadlec@gisat.cz',
	    'password': 'Earth6378'
	}

	results = query.query(
	    'Sentinel2',
	    start_date=datetime(2016, 1, 1),
	    end_date=datetime(2016, 12, 31),
	    geometry=Point(LON, LAT),
	)

	results = [r for r in results.values() 
		       if r["properties"]["title"].startswith("S2A_MSIL1C_")]
	results = sorted(results, key=lambda k: k['properties']['title'])

	for r in results:
		print(r["properties"]["title"], r["properties"]["cloudCover"], flush=True)
		if r["properties"]["cloudCover"] <= MAX_DLOUD:
			print(r["id"], flush=True)
			out_file = Path(OUTPUT_DIR).joinpath(r["properties"]["title"] + ".zip")
			if out_file.is_file():
				continue
			downloaded = False
			while not downloaded:
				# download single product by product ID
				try:
					download.download(r["id"], outfile=out_file, **CREDENTIALS)
					if out_file.stat().st_size < 100000:
						out_file.unlink()
						print('download incomplete..', flush=True)
						raise Exception
					downloaded = True
					print("{} downloaded successfully.".format(r["properties"]["title"]), flush=True)
				except Exception as e:
					sleep(10)
					print('download exception {}, retrying ..'.format(str(e)), flush=True)
					downloaded = False
			upload(DROPBOX_ACCESS_TOKEN, out_file, '/Apps/mysentinel2/' + out_file.name)

if __name__ == "__main__":
	main()