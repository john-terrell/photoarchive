import os
import sys
import logging
import couchdb
import exiftool
import subprocess
import uuid

extensions = (
    ".arw",
    ".cr2",
    ".dng",
    ".heic",
    ".jpg",
    ".jpeg",
    ".mov",
    ".mp4",
    ".nef",
    ".png",
    ".tif",
    ".tiff"
)

unique_identifier_keys = (
    "EXIF:RawDataUniqueID",
    "XMP:OriginalDocumentID",
)

generateduuid_value = 'Generated UUID'

def get_image_unique_id(metadata):
    id = ''
    derived_from = ''

    for unique_identifier_key in unique_identifier_keys:
        if unique_identifier_key in metadata:
            id = metadata[unique_identifier_key].upper()
            derived_from = unique_identifier_key
            break

    if id == '':
        id = uuid.uuid4().hex.upper()
        derived_from = generateduuid_value

    return id, derived_from

def get_file_metadata(et, file):
    imported_metadata = et.get_metadata(file)
    id, derived_from = get_image_unique_id(imported_metadata)

    metadata = {}
    binary_keys = []

    for key in imported_metadata.keys():
        value = imported_metadata[key]
        if type(value) is str and value.startswith("(Binary data"):
            binary_keys.append(key)
        else:
            split = key.split(':', 1)
            if len(split) == 1:
                # No split, add as-is
                metadata[key] = imported_metadata[key]
            else:
                if split[0] in metadata:
                    metadata[split[0]][split[1]] = imported_metadata[key]
                else:
                    metadata[split[0]] = dict([(split[1], imported_metadata[key])])
        
    return metadata, binary_keys, id, derived_from

def import_directory(walk_dir):
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Syncing directory {walk_dir}...')
    couch = couchdb.Server("http://user:password@10.0.80.81:5984/")
    db = couch['photoarchive']

    file_count = 0
    max_files = 1000
    finished = False

    found_extensions = {}

    with exiftool.ExifTool() as et:
        for root,d_names,f_names in os.walk(walk_dir):
            for f_name in f_names:
                if (f_name.startswith(".")):
                    continue

                _, file_extension = os.path.splitext(f_name.lower())
                if file_extension in found_extensions:
                    found_extensions[file_extension] += 1
                else:
                    found_extensions[file_extension] = 1

                if (f_name.lower().endswith(tuple(extensions)) == False):
                    continue

                file_path = os.path.join(root, f_name)
                relative_path = os.path.relpath(file_path, walk_dir)

                # Extract file metadata
                metadata, binary_keys, id, derived_from = get_file_metadata(et, file_path)
                existing_image = db.view('_all_docs', key=id)
                if len(existing_image) != 0:
                    print(f'File: {file_path} (ID Exists, Skipping)')
                    continue

                existing_images = db.view('_design/images/_view/bySourceFile', key=relative_path)
                if len(existing_images) != 0:
                    print(f'File: {file_path} (Source Path Exists, Skipping)')
                    continue

                print(f'File: {file_path}')

                photoarchive_specific = {}

                metadata['SourceFile'] = relative_path
                metadata['type'] = "image"
                metadata['_id'] = id
                photoarchive_specific['IDDerivedFrom'] = derived_from

                metadata['PhotoArchive'] = photoarchive_specific
                db.save(metadata)

                for key_with_binary_data in binary_keys:
                    process_result = subprocess.run(['exiftool', '-b', f'-{key_with_binary_data}', file_path],stdout=subprocess.PIPE)
                    db.put_attachment(metadata, process_result.stdout, key_with_binary_data)

                # Add the file itself as an attachment
                file = open(file_path, "rb")
                db.put_attachment(metadata, file, f_name)

                file_count += 1
                if (max_files > 0 and file_count >= max_files):
                    finished = True
                    break
            if (finished):
                break

    print(found_extensions)

def main():
    if (len(sys.argv) > 1):
        walk_dir = sys.argv[1]
    else:
        walk_dir = "."
    print(f'Main: syncing {walk_dir}')
    import_directory(walk_dir)

if __name__ == "__main__":
    import_directory("/Volumes/photography/Catalogs/John's Personal/John's Masters/1988")
