import setuptools
# https://packaging.python.org/tutorials/packaging-projects/

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pysea-google-ads", # Replace with your own username
    version="0.0.5",
    author="Eugene Komissarov",
    author_email="ekom@cian.ru",
    description="Google Ads base",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://ekomissarov@bitbucket.org/ekomissarov/google_ads.git",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: Linux",
    ],
    python_requires='>=3.7',
    install_requires=[
        'pysea-common-constants',
        'pysea-google-analytics',
        'appdirs>=1.4.4',
        'attrs>=19.3.0',
        'cached-property>=1.5.1',
        'cachetools>=4.1.1',
        'certifi>=2020.6.20',
        'chardet>=3.0.4',
        'defusedxml>=0.6.0',
        'google-api-core>=1.21.0',
        'google-api-python-client>=1.9.3',
        'google-auth>=1.19.0',
        'google-auth-httplib2>=0.0.4',
        'google-auth-oauthlib>=0.4.1',
        'googleads>=24.0.0',
        'googleapis-common-protos>=1.52.0',
        'httplib2>=0.18.1',
        'idna>=2.10',
        'isodate>=0.6.0',
        'lxml>=4.5.2',
        'oauth2client>=4.1.3',
        'oauthlib>=3.1.0',
        'protobuf>=3.12.2',
        'pyasn1>=0.4.8',
        'pyasn1-modules>=0.2.8',
        'pytz>=2020.1',
        'PyYAML>=5.3.1',
        'requests>=2.24.0',
        'requests-oauthlib>=1.3.0',
        'requests-toolbelt>=0.9.1',
        'rsa>=4.6',
        'six>=1.15.0',
        'uritemplate>=3.0.1',
        'urllib3>=1.25.9',
        'xmltodict>=0.12.0',
        'zeep>=3.4.0',

    ]
)