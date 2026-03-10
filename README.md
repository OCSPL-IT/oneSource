# Project Name  (oneSource)


- Python Django
- Tailwind
- Database : mssql


## Installation

Follow these steps to set up the project locally:

1. Clone the repository:
    ```bash
    git clone https://github.com/OCSPL/OneSource/tree/main
    cd https://github.com/OCSPL/OneSource/tree/main
    ```

2. Create and activate a virtual environment:
    ```bash
    python -m venv venv
    venv\Scripts\activate
    ```
3. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4. cd oneSource

5. Apply migrations (if applicable):
    ```bash
    python manage.py migrations
    python manage.py migrate
    ```
6. If you want your own superuser account run below commmand
    python manage.py createsuperuser

7. Start the development server:
    ```bash
    python manage.py runserver


## Usage

- Access the portal at http://127.0.0.1:8000
- Log in using default admin credentials (set in the admin panel).

