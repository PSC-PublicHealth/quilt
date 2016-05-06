#!/usr/bin/env python

from distutils.core import setup

setup(name='quilt',
      version='0.0.1',
      description='Framework for agent-based simulations based on greenlets',
      author='Joel Welling',
      author_email='welling@psc.edu',
      #url='https://www.python.org/sigs/distutils-sig/',
      packages=['quilt',
                ],
      package_dir = {'': 'src'}
     )
