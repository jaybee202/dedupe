# dedupe

The dedupe package helps you identify where and why where your data is duplicated...where it should not be.  As a full stack data professional (engineering, modeling, visualization, and science) identifying the source of duplicated values is a common task that can take some time (especially with hundreds of rows).  This time is better spent adding value.  In this repo you will find:


- dedupe.py: This is the package.  To use import polars, numpy (just to be safe), and the Dupe class object (from dedupe import Dupe)
- dedupe.ipynb: Jupyter notebook version of the package (includes example usage at the bottom)
- dedupe_example_usage.ipynb: This notebook shows you how the package works with two examples, one with duplicates and another without
- Superstore.csv: This is included for the example usage.  It does not inlcude any duplicated values.
- superstore_with_5_dupes.csv: This is the example file that inlcudes five duplicated rows.

I hope this package helps you out!
