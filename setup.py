from setuptools import setup

setup(name='dbstats',
		version='0.1',
		description='dbstats',
        url='https://github.com/jgeofil/dbstats',
        author='Jeremy Georges-Filteau',
        author_email='jeremy@thehyve.nl',
        license='MIT',
        packages=['funniest'],
		install_requires=[
			'psycopg2',
			'yaml',
			'sparse',
			'tqdm',
			'numpy'
        ],
        zip_safe=False)