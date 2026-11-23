# bdlaw
 This is a scrapped version of bdlaws
## How to RUN

```bash
python3 bdlaw.py serve
```
The database is stored as compressed parts (`bdlaws.db.gz.*`); `bdlaws.db` is rebuilt from them automatically on first run.
After `python3 bdlaw.py update`, run `python3 bdlaw.py split` to refresh the parts before committing.

## How to update the database from the site
```bash
# 1. Fetch acts added to the site since the last run (quick)
python3 bdlaw.py update
#    or re-download every act and replace the ones whose text changed (slow, ~1700 pages)
python3 bdlaw.py update --all

# 2. Rewrite the compressed parts so git sees the new data
python3 bdlaw.py split

# 3. Commit and push
git add bdlaws.db.gz.*
git commit -m "Update laws database"
git push
```
- If some acts fail, run `python3 bdlaw.py update` again; it only fetches what is still missing.
- To keep a log: `python3 bdlaw.py update 2>&1 | tee update.log`
- Run `update --all` rarely (e.g. monthly) to be polite to the government server.


Disclaimer:
This repo is just a clone of the original site. And It might be not always match the original site. The links to the original site is add in each page. Please take a look at that page first.
