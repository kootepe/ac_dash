# Flask application for validating static chamber measurements.

Proper implementation which uses this repo as a submodule:

## Running the app.


Clone the repo with:
```
git clone https://github.com/kootepe/ac_dash.git
```
or
```
git clone git@github.com:kootepe/ac_dash.git
```
Add file ```.env.dev``` with this content:
```
FLASK_APP=app.py
FLASK_DEBUG=1
DATABASE_URL=postgresql://hello_flask:hello_flask@db:5432/hello_flask_dev
SQL_HOST=db
SQL_PORT=5432
DATABASE=postgres
FLASK_CONFIG=ac_dash.db.Config
```
and run
```
docker compose up --build
```

