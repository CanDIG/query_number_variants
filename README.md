# query_number_variants

This script is used to fetch the number of variants given dataset_id, range, reference_name.

# Environment setup

Initialize a virtual environment with python 3.6+: `Python3 -m venv query_number_variants`

Git clone this repo: `git clone https://github.com/CanDIG/query_number_variants.git`

Install requests: `pip install requests`. This is the only external dependency.

# Modify the config and run the script

Locate the `query_number_variants_options.json` and change configs as you like.

Please note that the `server_address` should always point to the `/search` endpoint.

# Reminders

When running the script, it would output the number of variants at current search interval. If you see this number to be the same as your server's page_size limit (default to be 1800), it is likely that you are missing some results. In this case, change your server config. You should make all 3 configs listed below to be substantially bigger:

`MAX_CONTENT_LENGTH = 20 * 1024 * 1024`

`MAX_RESPONSE_LENGTH = 20 * 1024 * 1024`

`DEFAULT_PAGE_SIZE = 10000`

Since the size of data depends on how many variantsets you are querying at a time, and the density of variants, it is impossible to give a general advice on how small or big these numbers should be. If you are unsure, set the config mentioned above to something really really big, e.g., `DEFAULT_PAGE_SIZE` to be 100000, and both `MAX_CONTENT_LENGTH` and ` MAX_RESPONSE_LENGTH` to be `50 * 1024 * 1024`.

Additionally, you should consider reducing the increment value. As a general rule of thumb, the `increment` should not be bigger than `2000000`. If your dataset contains hundreds of variant sets, it would be a good idea to reduce it to something smaller. Depending on the range you query, it should not be too small either. Preferably bigger than `100000`.

Another good reference is each request (represented by one line of log that prints the number of variants from the most recently searched range) should not take more than 4 seconds. If it does, consider reducing the `increment` to something smaller.

# Output

The script will output 1 json file per ethnicity that you search for, and 1 json file that gives an overview of your search. It will be printed to a child directory, formatted as `output` with the current timestamp, e.g. `output_20191001_192232`.

# Known issues

- If the ethnicity you search for contains `/`, it is not properly escaped as of yet. Please do not search for these.
