Baseball Stat Collection
========================
Automate stat collection via boxscores

Installation
------------

.. code::

    virtualenv boxscores --no-site-packages
    source boxscores/bin/activate
    pip install -r pip-requires

Database Design
---------------

Tables

- Boxscore meta

    - year, team_name, season_schedule_link

- Boxscore

    - team_one, team_two, boxscore_url, date, path_to_html
