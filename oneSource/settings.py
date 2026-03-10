

from pathlib import Path
import os
from datetime import datetime
from django.utils import timezone
import logging
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv



# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(os.path.join(BASE_DIR, '.env'))



# Allow more POST fields (inline formset with many rows)
DATA_UPLOAD_MAX_NUMBER_FIELDS = 10000  # or any large number you’re comfortable with


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv('SECRET_KEY')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.getenv('DEBUG', 'False').lower() in ('true', '1', 't')

ALLOWED_HOSTS = os.getenv('ALLOWED_HOSTS', '').split(',')

# Application definition

INSTALLED_APPS = [
    # 'jazzmin',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django_extensions',
    "django_apscheduler",
    'main',   # django app name
    'HR',
    'EHS',
    'STORE',
    'STORE.material_issue_capex.apps.MaterialIssueCapexConfig',
    'PRODUCTION',
    'PRODUCTION.daily_block.apps.DailyBlockConfig', 
    'ETP',
    'ETP.MEE.apps.MeeConfig',
    'ETP.BIOREACTOR.apps.BioreactorConfig',
    'CANTEEN',
    'maintenance.apps.MaintenanceConfig',
    'django_browser_reload',   # browser auto reload
    'import_export',           # file import export from admin panel
    'django_select2',
    'UTILITY',
    'QC',
    'CREDENTIALS',
    'CONTRACT',
    'HR_BUDGET',
    'R_and_D',
    'REPORTS',
    "ERP_Reports.apps.ERPReportsConfig",
    'PERSONAL_CARE',
    'ACCOUNTS',
    'ACCOUNTS.Receivable.apps.ReceivableConfig',
    'ACCOUNTS.CASHFLOW.apps.CashflowConfig', 
    'ACCOUNTS.Budget.apps.BudgetConfig', 
    'IMPORT_EXPORT.Export_Commercial_Invoice.apps.Export_Commercial_InvoiceConfig',
    'PURCHASE.DomesticETA.apps.DomesticETAConfig',
    "SALES_MARKETING.sales_crm.apps.SalesCrmConfig",
]



MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',

    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',

    'django.contrib.auth.middleware.AuthenticationMiddleware',
     "main.middleware.CurrentUserMiddleware",
    # MUST be right after auth so request.user is available and fresh
    'main.middleware.LastSeenMiddleware',

    # Only once. Keep the one that reads from your log_filters helpers.
    'main.middleware.RequestLogContextMiddleware',

    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',

    # keep dev-only tools at the end
    'django_browser_reload.middleware.BrowserReloadMiddleware',
]



ROOT_URLCONF = 'oneSource.urls'

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [
            BASE_DIR / "ACCOUNTS" / "FixedAssets" / "templates",   # ✅ force include
        ],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "oneSource.context_processors.auth_extras",
                "ACCOUNTS.Budget.context_processors.budget_access_flags",
            ],
        },
    },
]

WSGI_APPLICATION = 'oneSource.wsgi.application'


# Database
# https://docs.djangoproject.com/en/5.0/ref/settings/#databases

# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.sqlite3',
#         'NAME': BASE_DIR / 'db.sqlite3',
#     }
# }

AUTHENTICATION_BACKENDS = [
    'django.contrib.auth.backends.ModelBackend',   # must be present
]


DATABASE_ROUTERS = ['main.db_router.ReadOnlyDBRouter']

DATABASES = {
    'default': {
        'ENGINE': 'mssql',
        'NAME': os.getenv('DB_DEFAULT_NAME'),
        'USER': os.getenv('DB_DEFAULT_USER'),
        'PASSWORD': os.getenv('DB_DEFAULT_PASSWORD'),
        'HOST': os.getenv('DB_DEFAULT_HOST'),
        'PORT': os.getenv('DB_DEFAULT_PORT'),
        'OPTIONS': {
            'driver': 'ODBC Driver 17 for SQL Server',
            'extra_params': 'TrustServerCertificate=yes;',
            'trusted_connection': 'yes',
        },
    },
    'readonly_db': {
        'ENGINE': 'mssql',
        'NAME': os.getenv('DB_READONLY_NAME'),
        'USER': os.getenv('DB_READONLY_USER'),
        'PASSWORD': os.getenv('DB_READONLY_PASSWORD'),
        'HOST': os.getenv('DB_READONLY_HOST'),
        'PORT': os.getenv('DB_READONLY_PORT'),
        'OPTIONS': {
            'driver': 'ODBC Driver 17 for SQL Server',
            'extra_params': 'TrustServerCertificate=yes;',
            'trusted_connection': 'yes',
        },
    },
    'production_scheduler': {
        'ENGINE': 'mssql',
        'NAME': os.getenv('DB_PRODSCHED_NAME'),
        'USER': os.getenv('DB_PRODSCHED_USER'),
        'PASSWORD': os.getenv('DB_PRODSCHED_PASSWORD'),
        'HOST': os.getenv('DB_PRODSCHED_HOST'),
        'PORT': os.getenv('DB_PRODSCHED_PORT'),
        'OPTIONS': {
            'driver': 'ODBC Driver 17 for SQL Server',
            'extra_params': 'TrustServerCertificate=yes;',
            'trusted_connection': 'yes',
        },
    },
}





# Password validation
# https://docs.djangoproject.com/en/5.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/5.0/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'Asia/Kolkata'

USE_I18N = True
USE_TZ = True




# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.0/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'                                                 #WHen using whitenoice need to do this setting
# STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"      #WHen using whitenoice need to do this setting
STATICFILES_STORAGE = 'django.contrib.staticfiles.storage.StaticFilesStorage'          #WHen using whitenoice need to do this setting

# Default primary key field type
# https://docs.djangoproject.com/en/5.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'



# Unauthencated User redirected below path
LOGIN_URL = '/'
LOGOUT_REDIRECT_URL = '/'  



#Email Configuration
EMAIL_BACKEND = os.getenv('EMAIL_BACKEND', 'django.core.mail.backends.smtp.EmailBackend')
EMAIL_HOST = os.getenv('EMAIL_HOST', 'smtp-mail.outlook.com')
EMAIL_PORT = int(os.getenv('EMAIL_PORT', 587))
EMAIL_USE_TLS = os.getenv('EMAIL_USE_TLS', 'True').lower() in ('true', '1', 't')
EMAIL_HOST_USER = os.getenv('EMAIL_HOST_USER', '')
EMAIL_HOST_PASSWORD = os.getenv('EMAIL_HOST_PASSWORD', '')
DEFAULT_FROM_EMAIL = os.getenv('DEFAULT_FROM_EMAIL', EMAIL_HOST_USER)


# === REDIS / CELERY settings ===
CELERY_BROKER_URL = "redis://127.0.0.1:6379/0"
CELERY_RESULT_BACKEND = "redis://127.0.0.1:6379/0"  
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TASK_SERIALIZER = 'json'
CELERY_TIMEZONE = 'Asia/Kolkata'
CELERY_ENABLE_UTC = False
CELERY_TASK_ALWAYS_EAGER = False  # keep False in prod

CELERY_TASK_IGNORE_RESULT = True
CELERY_RESULT_EXPIRES = 120

# === Celery logging prefs ===
CELERY_WORKER_HIJACK_ROOT_LOGGER = False   # don't replace Django logging
CELERY_REDIRECT_STDOUTS = False
CELERY_REDIRECT_STDOUTS_LEVEL = "INFO"






"""==== Logging Related ===="""


BASE_LOG_DIR = os.path.join(BASE_DIR, 'logs')


def get_log_dir():
    now = timezone.localtime(timezone.now())
    month_str = now.strftime('%Y-%B')         # '2025-June'
    today_str = now.strftime('%Y-%m-%d')      # '2025-06-19'
    month_dir = os.path.join(BASE_LOG_DIR, month_str)
    log_dir = os.path.join(month_dir, today_str)
    os.makedirs(log_dir, exist_ok=True)
    return log_dir

class DailyFolderTimedRotatingFileHandler(TimedRotatingFileHandler):
    """Writes logs to a daily folder, rotates at midnight, always uses new folder after 12AM."""
    def _open(self):
        # On every rotate, recalculate the log_dir
        self.baseFilename = os.path.join(get_log_dir(), os.path.basename(self.baseFilename))
        return super()._open()



def skip_unimportant_404s(record):
    """
    Filter out 404 errors for paths that are not critical.
    """
    # The log record for django.server requests has args: (request_line, status_code, content_length)
    if record.name == 'django.server' and isinstance(record.args, tuple) and len(record.args) == 3:
        status_code = record.args[1]
        request_path = record.args[0].split(' ')[1]  # Get the path from "GET /path HTTP/1.1"

        # List of paths to ignore for 404 errors
        ignored_paths = [
            '/static/',
            '/.well-known/',
            '/favicon.ico',
        ]

        if status_code == '404' and any(request_path.startswith(p) for p in ignored_paths):
            return False
        if status_code == '200' and any(request_path.startswith(p) for p in ignored_paths):
            return False
            
    return True


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'skip_unimportant_404s': {
            '()': 'django.utils.log.CallbackFilter',
            'callback': skip_unimportant_404s,
        }
    },
    'formatters': {
        'verbose': {
            'format': '[{asctime}] {levelname} {name} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname}: {message}',
            'style': '{',
        },
        # Celery: use % style so we can reference %(celery_task_id)s etc.
        'celery_verbose': {
            'format': '[%(asctime)s] %(levelname)s %(processName)s %(name)s '
                    'task_id=%(celery_task_id)s task_name=%(celery_task_name)s %(message)s',
            'style': '%',
        },
    },
    'handlers': {
        'access_file': {
            'level': 'INFO',
            'class': 'oneSource.settings.DailyFolderTimedRotatingFileHandler',  # Update the path to wherever you put the class
            'filename': os.path.join(get_log_dir(), 'access.log'),
            'when': 'midnight',
            'backupCount': 30,
            'formatter': 'verbose',
            'encoding': 'utf8',
        },
        'error_file': {
            'level': 'ERROR',
            'class': 'oneSource.settings.DailyFolderTimedRotatingFileHandler',
            'filename': os.path.join(get_log_dir(), 'error.log'),
            'when': 'midnight',
            'backupCount': 30,
            'formatter': 'verbose',
            'encoding': 'utf8',
        },
        'debug_file': {
            'level': 'DEBUG',
            'class': 'oneSource.settings.DailyFolderTimedRotatingFileHandler',
            'filename': os.path.join(get_log_dir(), 'debug.log'),
            'when': 'midnight',
            'backupCount': 30,
            'formatter': 'verbose',
            'encoding': 'utf8',
        },
        'warning_file': {
            'level': 'WARNING',
            'class': 'oneSource.settings.DailyFolderTimedRotatingFileHandler',
            'filename': os.path.join(get_log_dir(), 'warning.log'),
            'when': 'midnight',
            'backupCount': 30,
            'formatter': 'verbose',
            'encoding': 'utf8',
        },
            'critical_file': {
                'level': 'CRITICAL',
                'class': 'oneSource.settings.DailyFolderTimedRotatingFileHandler',
                'filename': os.path.join(get_log_dir(), 'critical.log'),
                'when': 'midnight',
                'backupCount': 30,
                'formatter': 'verbose',
                'encoding': 'utf8',
            },
            'celery_worker_file': {
            'level': 'INFO',
            'class': 'oneSource.settings.DailyFolderTimedRotatingFileHandler',
            'filename': os.path.join(get_log_dir(), 'celery_worker.log'),
            'when': 'midnight',
            'backupCount': 30,
            'formatter': 'celery_verbose',
            'encoding': 'utf8',
        },
        'celery_beat_file': {
            'level': 'INFO',
            'class': 'oneSource.settings.DailyFolderTimedRotatingFileHandler',
            'filename': os.path.join(get_log_dir(), 'celery_beat.log'),
            'when': 'midnight',
            'backupCount': 30,
            'formatter': 'celery_verbose',
            'encoding': 'utf8',
        },
        'null': {
        'class': 'logging.NullHandler',
    },
    },
    'loggers': {
        'django': {
            'handlers': [
                'access_file',
                'error_file',
                'debug_file',
                'warning_file',
                'critical_file',
            ],
            'level': 'DEBUG',
            'propagate': True,
        },
        'django.request': {
            'handlers': ['error_file'],
            'level': 'ERROR',
            'propagate': False,
        },
        'django.db.backends': {
            'handlers': ['debug_file'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'custom_logger': {
            'handlers': [
                'access_file',
                'error_file',
                'debug_file',
                'warning_file',
                'critical_file',
            ],
            'level': 'DEBUG',
            'propagate': False,
        },
        'HR': {  # This will catch logs from "HR.tasks"
            'handlers': ['celery_worker_file', 'error_file'],
            'level': 'INFO',
            'propagate': False, # Important: prevent logs from going to parent loggers
        },
        # NEW: core Celery logger (all celery internals)
        'celery': {
            'handlers': ['celery_worker_file', 'error_file'],
            'level': 'INFO',
            'propagate': False,
        },
        # NEW: task runtime traces (when a task runs/fails)
        'celery.app.trace': {
            'handlers': ['celery_worker_file', 'error_file'],
            'level': 'INFO',
            'propagate': False,
        },
        # NEW: worker process messages
        'celery.worker': {
            'handlers': ['celery_worker_file'],
            'level': 'INFO',
            'propagate': False,
        },
        # NEW: beat scheduler messages
        'celery.beat': {
            'handlers': ['celery_beat_file'],
            'level': 'INFO',
            'propagate': False,
        },
        #  'django.server': {
        #     'handlers': ['access_file', 'warning_file'],
        #     'level': 'INFO',
        #     'filters': ['skip_unimportant_404s'], # <-- Use the new filter here
        #     'propagate': False,
        # },
          # --- SILENCE UNWANTED LOGS ---
        'django.server': {
            'handlers': ['null'],
            'propagate': False,
        },
        'django.utils.autoreload': {
            'handlers': ['null'],
            'propagate': False,
        },
         
    },
}



