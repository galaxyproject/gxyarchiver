# GXYArchiver

GXYArchiver is a Python script that interacts with the Galaxy API to archive
histories out of galaxy, and stage them into large bundles for sending to long
term storage (like tape).

It needs to write to a galaxy filesource.  Example basic setup is:

```
- type: posix
  root: '/home/dannon/work/galaxyarchiver/export/'
  requires_roles: galaxy-archiver-role
  id: gxy-archiver
  doc: Galaxy history archive export destination
  label: "archives"
  writable: true
```

## Usage

You can run `gxyarchiver.py` from the command line as follows:

Single history archive:

```
GALAXY_API_URL="http://localhost:8080/api" GALAXY_API_KEY=admin-api-key python3 gxyarchiver.py archive --history-id=c11b5ebb6125
```

Multiple history archives:

```
 GALAXY_API_URL="http://localhost:8080/api" GALAXY_API_KEY=admin-api-key python3 gxyarchiver.py archive --history-id-file=historiestoarchive.txt
 ```

Where `historiestoarchive.txt` is a file containing a list of history IDs, one per line, like:

```
c11b5ebb6125
c11b5ebb6126
c11b5ebb6127
```


Once this is done, you should have archives in wherever your filesource is configured to write to.  You can then stage them with:

```

Bundling:

```
python3 gxyarchiver.py bundle --folder-path=archive
```

This will check through archive/export, and create a bundle of histories adding up to the specified size, writing a manifest of all contents for searchability.  It will then move the histories into archive/bundled, and so on.
