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

The Teamleader refresh token needed to run this Prefect flow is stored in the `teamleader-credentials` Prefect block (TeamleaderCredentials), next to the client id and secret. The flow replaces it on every token refresh; the access token is only kept in memory during a run. In case the refresh token is invalidated, you must manually grant authorization to Teamleader to generate a new one.

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
This will redirect you to the Teamleader authorization page and save the newly generated refresh token to the `teamleader-credentials` Prefect block.
If you receive an error while authorizing, stop the server from your terminal with `Ctrl+C` and restart it with the above command.
