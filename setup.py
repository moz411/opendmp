"""
    setup.py

    Setup script for opendmp

    Copyright (c) 2012 Thomas DUPOUY <moz@gmx.fr>.
    All rights reserved.

    Redistribution and use in source and binary forms, with or without modification,
    are permitted provided that the following conditions are met:

    1. Redistributions of source code must retain the above copyright notice, 
    this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright 
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.

    3. Neither the name of Thomas DUPOUY nor the names of other contributors may 
    be used to endorse or promote products derived from this software without
    specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
    ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
    (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
    ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

from distutils.core import setup

setup(
    name = 'opendmp',
    version = '0.1.0',
    description = 'Open-source implementation of the NDMP v4 protocol.',
    author = 'Thomas DUPOUY',
    author_email = 'moz@gmx.fr',
    url = 'http://moz.free.fr/opendmp',
    license = 'GPL v3',
    packages=['opendmp', 'opendmp.bu', 'opendmp.extensions', 'opendmp.interfaces', 'opendmp.opendmp.conf', 'opendmp.server', 'opendmp.tests', 'opendmp.tools', 'opendmp.xdr'],
    package_dir={'': '..'},
    scripts = [''],
)
