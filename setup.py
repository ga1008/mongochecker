import sys

import setuptools
from setuptools import setup

using_setuptools = True

#
# with open('scrapydartx/VERSION') as f:
#     version = f.read().strip()

setup_args = {
    'name': 'mongochecker',
    'version': '0.0.1',
    'url': 'https://github.com/ga1008/mongochecker',
    'description': 'a tool to remove mongodb duplicate data',
    'long_description': open('README.md', encoding="utf-8").read(),
    'author': 'Guardian',
    'author_email': 'zhling2012@live.com',
    'maintainer': 'Guardian',
    'maintainer_email': 'zhling2012@live.com',
    'long_description_content_type': "text/markdown",
    'LICENSE': 'MIT',
    'packages': setuptools.find_packages(),
    'include_package_data': True,
    'zip_safe': False,
    'entry_points': {
        'console_scripts': ['mongochecker = MongodbDuplicateChecker.delete_duplicate:starter']
        },

    'classifiers': [
                    "Programming Language :: Python :: 3",
                    "License :: OSI Approved :: MIT License",
                    "Operating System :: OS Independent",
                ],
}

setup(**setup_args, install_requires=['tqdm', 'pymongo'])
