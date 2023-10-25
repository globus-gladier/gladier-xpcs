"""
Django settings for xpcs_portal project.

Generated by 'django-admin startproject' using Django 3.2.6.

For more information on this file, see
https://docs.djangoproject.com/en/3.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.2/ref/settings/
"""

from pathlib import Path
# Set SEARCH_INDEXES by importing it from the app instead
from xpcs_portal.xpcs_index.apps import SEARCH_INDEXES  # noqa

try:
    from concierge_app import CONCIERGE_SCOPE
    import globus_automate_client
    extra_scopes = [globus_automate_client.flows_client.MANAGE_FLOWS_SCOPE,
                    CONCIERGE_SCOPE]
except ImportError:
    extra_scopes = []

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.2/howto/deployment/checklist/

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True
ALLOWED_HOSTS = []


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.sitemaps',
    'django.contrib.staticfiles',
    'django.contrib.humanize',
    # This contains general Globus portal tools
    'crispy_forms',
    'crispy_bootstrap4',
    'globus_portal_framework',
    'social_django',
    'automate_app',
    'globus_app_flows',
    'concierge_app',
    'xpcs_portal.xpcs_index',
    'alcf_data_portal',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'social_django.middleware.SocialAuthExceptionMiddleware',
    'globus_portal_framework.middleware.GlobusAuthExceptionMiddleware',
    'globus_portal_framework.middleware.ExpiredTokenMiddleware',
]

LOGIN_URL = '/login/globus'
AUTHENTICATION_BACKENDS = [
    'globus_portal_framework.auth.GlobusOpenIdConnect',
    'django.contrib.auth.backends.ModelBackend',
]

ROOT_URLCONF = 'xpcs_portal.testing.urls'

# This copies the ALCFDataPortal, which by default still uses the old templates
BASE_TEMPLATES = ''
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            BASE_DIR / 'testing' / 'templates',
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'social_django.context_processors.backends',
                'social_django.context_processors.login_redirect',
                'globus_portal_framework.context_processors.globals',
                # 'alcf_data_portal.context_processors.globals',
            ],
        },
    },
]
CRISPY_ALLOWED_TEMPLATE_PACKS = "bootstrap4"
CRISPY_TEMPLATE_PACK = "bootstrap4"


SOCIAL_AUTH_GLOBUS_SCOPE = [
    'urn:globus:auth:scope:search.api.globus.org:all',
    'urn:globus:auth:scope:transfer.api.globus.org:all',
    'urn:globus:auth:scope:groups.api.globus.org:view_my_groups_and_memberships',  # noqa
    # Scope for xpcs HTTPS data on an Eagle Shared Endpoint.
    'https://auth.globus.org/scopes/74defd5b-5f61-42fc-bcc4-834c9f376a4f/https',  # noqa
    # Note: Automate scopes are only added if the globus-automate-client is installed
] + extra_scopes

ALLOWED_FRONTEND_TOKENS = [
    '74defd5b-5f61-42fc-bcc4-834c9f376a4f',
]


# Database
# https://docs.djangoproject.com/en/3.2/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    "formatters": {
        "basic": {
            "format": "[%(levelname)s] " "%(name)s::%(funcName)s() %(message)s"
        }
    },
    'handlers': {
        'stream': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'basic',
        },
    },
    'loggers': {
        'django': {'handlers': ['stream'], 'level': 'INFO'},
        'django.db.backends': {'handlers': ['stream'], 'level': 'WARNING'},
        'globus_portal_framework': {'handlers': ['stream'], 'level': 'DEBUG'},
        'xpcs_portal': {'handlers': ['stream'], 'level': 'DEBUG', 'propagate': True},
        'automate_app': {'handlers': ['stream'], 'level': 'DEBUG', 'propagate': True},
        'concierge_app': {'handlers': ['stream'], 'level': 'DEBUG', 'propagate': True},
        'globus_app_flows': {'handlers': ['stream'], 'level': 'DEBUG', 'propagate': True},
    },
}

# Internationalization
# https://docs.djangoproject.com/en/3.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.2/howto/static-files/

STATIC_URL = '/static/'

# Default primary key field type
# https://docs.djangoproject.com/en/3.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'


try:
    from xpcs_portal.testing.local_settings import *  # noqa
except ImportError:
    raise
