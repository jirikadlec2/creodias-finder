import dropbox
import os

DROPBOX_ACCESS_TOKEN = 'oKFU0w6IHvEAAAAAAAFxV-NhqpjyV4fRJGwBovhIN4kSDj0Ko8YqTDWA71Omrh8D'
DROPBOX_BASE_PATH = '/Apps/mysentinel2'
DOWNLOAD_DIR = '/mnt/freenas_pracovni_archiv_01/Sentinel-2/MSI/2016/S2A/S2MSI1C/'

TILES = ['34UDV', '33UXP']

dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
for tile in TILES:
    target_dir = os.path.join(DOWNLOAD_DIR, tile)
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)
    files = dbx.files_list_folder(path=DROPBOX_BASE_PATH + '/' + tile)
    for f in files.entries:
        target_file = os.path.join(target_dir, f.name)
        if not os.path.exists(target_file):
            dbx.files_download_to_file(target_file, f.path_lower)
            cmd2 = "unzip -o" + " " + target_file + " -d " + target_dir
            os.system(cmd2)
