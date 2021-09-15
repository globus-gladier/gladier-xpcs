import logging.config

logging.config.dictConfig({
    'version': 1,
    'formatters': {
        'basic': {'format': '[%(levelname)s] '
                            '%(name)s::%(funcName)s() %(message)s'}
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'basic',
        }
    },
    'loggers': {
        'gladier': {'level': 'INFO', 'handlers': ['console']},
        'gladier_xpcs': {'level': 'DEBUG', 'handlers': ['console']},
    },
})
