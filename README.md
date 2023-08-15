# FPL Draft League
---
I picked up this hobby project from [leej11/fpl_draft_league](https://github.com/leej11/fpl_draft_league) ⚽️

The starter code in this project from [leej11](https://github.com/leej11) provided a solid foundation of calling the Premier League's Fantasy API and creating nice chart visualizations for user groups.

I have extended the project to email myself a generated report of players that have been added to the league and any injury/transfer status updates of players, as a scheduled job.

The job runs daily using *cron*. It is scheduled on my local machine and works to compare the api information of the previous day from an updated *sqlite* table with the currently fetched api extracts. 

An example of the configuration file/variables required to run the project are located in [src/configuration/example-config.json](src/configuration/example-config.json).

The entry point program that gets run when being scripted is [src/main.py](src/main.py).

## Requirements

I am currently running this project on *Python 3.9.16* with all my downloaded packages installed with pip and frozen to [requirements.txt](requirements.txt).

After downloading the correct version of Python and creating a virtual environment, you can easily setup these packages with this command: `pip3 install -r requirements.txt`.
