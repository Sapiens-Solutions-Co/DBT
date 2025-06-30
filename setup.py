#!/usr/bin/env python
#!/usr/bin/env python
import os
import sys

from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()

package_name = "dbt-proplum"
package_version = '1.0.0'
description = """The proplum adapter plugin for dbt (data build tool)"""

def get_package_data():
    """Return package data based on the specified adapter."""
    return {
        "dbt": [
            "include/greenplum/macros/materializations/external_table.sql",
            "include/greenplum/macros/materializations/proplum.sql",
            "include/greenplum/macros/proplum/*.sql",
            "include/clickhouse/macros/*.sql",
            "include/clickhouse/macros/**/*.sql",
            "include/global_project/macros/proplum/*.sql", 
        ]
    }

package_data = get_package_data()

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Sapiens",
    author_email="a.koshelev@sapiens.solutions",
    url="https://gitlab.sapiens.solutions/proplum/dbt-adapter",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    package_data=package_data,  
    include_package_data=True,
    install_requires=[],
    zip_safe=False,
    classifiers=[],
    python_requires=">=3.7",
)

