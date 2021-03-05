import setuptools
from setuptools import setup

using_setuptools = True

setup_args = {
    'name': 'mongocheck',
    'version': '0.0.10',
    'url': 'https://github.com/ga1008/mongochecker',
    'description': 'a tool to remove mongodb duplicate data or copy mongodb data',
    # 'long_description': open('README.md', encoding="utf-8").read(),
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
        'console_scripts': [
            'mongocheck = MongodbDuplicateChecker.delete_duplicate:dl_starter',
            'mongocopy = MongodbDuplicateChecker.delete_duplicate:cp_starter'
        ]
    },

    'classifiers': [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    'install_requires': [
        'tqdm',
        'pymongo',
        'basecolors==0.0.2'
    ],
}

setup(**setup_args)
