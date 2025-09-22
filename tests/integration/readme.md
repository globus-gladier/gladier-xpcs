
### Integration tests for XPCS


### Test Datasets

This tests 3 different datasets in Multitau and Twotime:

* Rigaku3m
    * Raw files include 6 bins, and one HDF
* Rigaku500k
    * Raw files include 1 bin file
* Eiger4m
    * Raw files include one H5, plus the HDF metadata

This will run each of the datasets set in DATASETS in the test_datasets.py module. Note that all tests
have special cycle and experiments set, and will publish to the following area under 2025-02/integraiton-test202507/:

https://app.globus.org/file-manager?origin_id=74defd5b-5f61-42fc-bcc4-834c9f376a4f&origin_path=%2FXPCSDATA%2FAutomate%2F2025-2%2F&two_pane=false

Suggest running with the following:

```
    pytest -o log_cli=true -m integration --integration -n 8
```

Notes on this command:

* `-o log_cli=true` will show live output on datasets
* `--integration` Required to run integration tests
* `-n 8` will use 8 workers (using pytest-xdist), so that tests are run in paralell.
* `-m integration` Will only run integration tests (Optional)