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

By default, Galaxy fails the export of a history that has a dataset whose file is missing from the object store,
and the history is left unarchived. Pass `--tolerate-missing-data` to send `ignore_errors` with the export
request: Galaxy then exports those datasets as discarded (attributes kept, no file) and skips missing provenance
instead of failing. This needs a Galaxy release that supports it for missing files.


Once this is done, you should have archives in wherever your filesource is configured to write to.  You can then stage them with:

```

Bundling:

```
python3 gxyarchiver.py bundle --folder-path=archive
```

This will check through archive/export, and create a bundle of histories adding up to the specified size, writing a manifest of all contents for searchability.  It will then move the histories into archive/bundled, and so on.

Histories that were exported but never archived:

If the `/archive` request fails, or is never made, after an export succeeds, the history keeps an up-to-date export
record. Later `archive` runs skip it, and `bundle --quarantine` / `verify --quarantine-path` move its zip to
`archive/quarantine`. As long as the history hasn't changed, that export is still valid. To archive these histories
with their existing export:

```
python3 gxyarchiver.py reconcile --folder-path=archive               # dry run: report what would be archived
python3 gxyarchiver.py reconcile --folder-path=archive --no-dry-run  # archive and purge them
```

For each zip in `archive/quarantine` whose history is not archived, `reconcile` checks that the zip is the history's
up-to-date export and that it holds a file of the right size for every dataset the history still has (state ok, not
purged). If so, it moves the zip back to `archive/export` and archives the history with that export, purging it.
Anything else stays in quarantine, with the reason printed. Passing `--folder-path` to `archive` does the same for a
history whose up-to-date export zip is in `archive/export` or `archive/quarantine`, instead of skipping it.
