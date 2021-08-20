# XPCS Portal

The XPCS portal displays all results in the XPCS Globus Search index.
All published results from the Gladier flows end up in the search index
and are searchable by this portal. Additionally, the portal allows searching
on these results and reprocessing them as needed.

#### WARNING
- - - -
    Note: The alcf-data-poral dependency is a temporary fix to allow running
    this portal standalone. The alcf-data-portal is not a package, and was
    never intended to be installed as such. This portal should be considered
    highly unstable until these features are ported into the main package.
- - - -


### Installation

Install the requirements with the following: 

```
pip install -r portal-requirements.txt
```

Install dependent components from the alcf portal with the following:

```
git clone https://github.com/globusonline/django-alcf-data-portal.git
cd django-alcf-data-portal.git
python setup.py develop
```


Create the file below with Globus App credentials. These are required for
login to work: 

```
# Place this file in xpcs_portal/testing/local_settings.py
SECRET_KEY = 'you can do `openssl rand -hex 32` or just leave this as-is'
SOCIAL_AUTH_GLOBUS_KEY = 'Your client id'
SOCIAL_AUTH_GLOBUS_SECRET = 'Your client secret'
```

With your `local_settings.py` file setup above, run the following to start
your local portal:

```
cd xpcs_portal/
python manage.py migrate
python manage.py runserver
```

Your portal should now be running at http://localhost:8000