# pdns

Command line tool to add and and delete dns records and domains
from powerdns postgresql database.

## Installation

Copy pdnscmd.conf.sample to /etc/pdnscmd.conf and edit as needed.

Create group pdnscmd and add your user(s) to this group.

Set /etc/pdnscmd.conf filesystem permissions to for example 0750
and change group to pdnscmd

    cp pdnscmd.conf.sample /etc/pdnscmd.conf
    vi /etc/pdnscmd.conf
    groupadd -r pdnscmd
    usermod -aG pdnscmd youruser
    chmod 750 /etc/pdnscmd.conf
    chgrp pdnscmd /etc/pdnscmd.conf

## Usage

Example usage.

    $ pdns.py
    > domain example.com
    example.com> add test A 127.0.0.1
    example.com> show
    ADD record name=test.example.com and type=A and content=127.0.0.1 and ttl=360
    example.com> commit
    example.com> list
    ...
    testi.example.com                360    A    -  127.0.0.1
    ...
    example.com> delete test.example.com A 127.0.0.1
    example.com> show
    DELETE record name=test.example.com and type=A and content=127.0.0.1
    example.com> revert

