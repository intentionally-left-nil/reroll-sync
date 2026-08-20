# Decisions
1. Create a pypi_index table, with the following keys: `name` (primary_key), `serial`, `updated_at`
2. A row is only added or updated from this table when every filename for that package has been inserted into the wheels table (all the other fields may be NULL but we at least need the filename)


# Update algorithm
1. Store the start_time. If at any time the elapsed_time exceeds the duration, stop. The next iteration of the index job will update the data
2. Call the simple index API
3. SELECT name, serial FROM pypi_index to get an in-memory representation of all the completed items
4. Filter out to have the names where name is either missing, or the serial is newer than the one in the database
5. For each outdated name:
    1. Query the projects API for the filename
    2. For each row, INSERT INTO `wheels` filename, project, pypi_simple ON CONFLICT IGNORE if the filename type is `%.whl`
6. Once the project is updated, `INSERT INTO pypi_index name, updated_at, serial **From the project page** not the index api, ON CONFLICT UPDATE updated_at`

If there is any partial failure, just stop processing the project and move onto the next project. The insert into pypi_index won't proceed, so the next run will try again

# Pypi index API
Reference: https://docs.pypi.org/api/index-api/

```
GET /simple/ HTTP/1.1
Host: pypi.org
Accept: application/vnd.pypi.simple.v1+json
```

returns a json object with two keys, `meta` and `projects`
```json
{
  "meta": {
    "_last-serial": 24888689,
    "api-version": "1.4"
  },
  "projects": [
    {
      "_last-serial": 3075854,
      "name": "0"
    }
  ]
}
```

# Pypi project api
```
GET /simple/beautifulsoup4/ HTTP/1.1
Host: pypi.org
Accept: application/vnd.pypi.simple.v1+json
```

retuns a json object with `files`, `meta` and other info
```json
{
"files": [
    {
      "core-metadata": false,
      "data-dist-info-metadata": false,
      "filename": "beautifulsoup4-4.0.1.tar.gz",
      "hashes": {
        "sha256": "dc6bc8e8851a1c590c8cc8f25915180fdcce116e268d1f37fa991d2686ea38de"
      },
      "requires-python": null,
      "size": 51024,
      "upload-time": "2014-01-21T05:35:05.558877Z",
      "url": "https://files.pythonhosted.org/packages/6f/be/99dcf74d947cc1e7abef5d0c4572abcb479c33ef791d94453a8fd7987d8f/beautifulsoup4-4.0.1.tar.gz",
      "yanked": false
    },
"meta": {
    "_last-serial": 22406780,
    "api-version": "1.4"
  }
}
```
