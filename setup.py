from setuptools import setup

setup(name='dbstats',
		version='0.2.2',
		description='dbstats',
        url='https://github.com/jgeofil/dbstats',
        author='Jeremy Georges-Filteau',
        author_email='jeremy@thehyve.nl',
        license='MIT',
        packages=['dbstats'],
		python_requires='>=3.0',
		install_requires=[
			'psycopg2',
			'sparse',
			'tqdm',
			'numpy'
        ],
        scripts=['bin/get-transmart-stats.py'],
        zip_safe=False)