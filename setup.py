from setuptools import setup

setup(
    name = 'DBToolBox',
    version = '0.0.5',
    author = 'Martin Arroyo',
    author_email = 'martinm.arroyo7@gmail.com',
    packages = ['DBToolBox'],
    install_requires = [
        'pandas',
        'python-dotenv',
        'psycopg2-binary',
        'SQLAlchemy',
        'pytest'
    ]
)