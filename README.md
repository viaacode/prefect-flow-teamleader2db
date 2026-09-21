# Teamleader2db

This [Prefect](https://www.prefect.io/) flow performs a one-way synchronization of the [Teamleader](https://www.teamleader.eu/) data to the elt_harvest database.

## Usage
Start a new flow run or configure a deployment to run periodically on the Prefect server.

Alternatively, install the requirements
```sh
pip install -r requirements-dev.txt
```

And start a run locally
```sh
python flows/main_flow.py
```

## Authorization

The Teamleader tokens needed to run this Prefect flow are stored in the etl_harvest database. In case they are invalidated, you must manually grant authorization to Teamleader to generate new tokens.

Install openssl from your favorite package manager (Teamleader only allows redirects to HTTPS servers so a certificate is created for the local server)
```
nix-shell -p openssl
```

Or any of the other package managers `apt`, `pacman`, ...

Clone the repository

```sh
git clone git@github.com:viaacode/teamleader2db.git
```

Install the requirements for authentication

```sh
pip install -r requirements-auth.txt
```

Set the following environment variables

```sh
# Teamleader integration credentials. Client refers here to the specific teamleader2db integration/plugin that is registered with Teamleader.
TL_CLIENT_ID
TL_CLIENT_SECRET

# etl harvest database credentials
POSTGRES_PORT
POSTGRES_DATABASE
POSTGRES_HOST
POSTGRES_USERNAME
POSTGRES_PASSWORD
```

Start the local server

```
python flows/authorization.py
```
This will redirect you to the Teamleader authorization page and save the newly generated tokens to the etl harvest database.
If you receive an error while authorizing, stop the server from your terminal with `Ctrl+C` and restart it with the above command.
