
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
    pytest tests/integration -o log_cli=true -m integration -n 6
```

Notes on this command:

* `-o log_cli=true` will show live output on datasets
* `-m integration` will only select the integration tests
* `-n 6` will use 6 workers (using pytest-xdist), so that tests are run in paralell.
