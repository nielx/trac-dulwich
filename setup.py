#!/usr/bin/env python

from setuptools import setup

setup(name='TracDulwich',
      install_requires='Trac >= 0.12',
      description='GIT version control plugin, with the pure-python Dulwich module, for Trac 0.12',
      author='Niels Sascha Reedijk',
      author_email='niels.reedijk@gmail.com',
      keywords='trac scm plugin git dulwich',
      url="http://",
      version='0.1.0',
      license="MIT",
      long_description="",
      packages=['trac_dulwich'],
      entry_points = {'trac.plugins': 'dulwich = trac_dulwich.dulwich_fs'},
)
