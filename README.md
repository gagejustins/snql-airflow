# SNQL-airflow
This is my fork of the puckel/docker-airflow repo. I'm going to (attempt to) get it running on the SNQL data I'm collecting. For inscrutable and unknown reasons, the original docker image and build don't work in a few different ways:

* The environment variables exported in scripts/entrypoint.sh do not work
* The commands to run the scheduler in scripts/entrypoint.sh do not work

To get Airflow running, you'll need to do two things:

* Create a scripts/setup.sh file that (a) exports a Fernet Key as an environment variable and (b) adds your Airflow connections (`airflow connections -a --conn_id --conn_uri`)
* Run the Airflow scheduler inside the container(s) you create (`airflow scheduler`)

It's not how Docker is supposed to work, but waddaya gonna do.
