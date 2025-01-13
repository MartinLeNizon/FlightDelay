# Usage

## Requirements

* To have docker *and* docker-compose installed.
* Install docker and docker-compose exactly as it is described in the website.
* **do not do do apt install docker or docker-compose**

## How to spin the webserver up

### Prepping

First, get your **id**:
```sh
id -u
```

Now edit the **.env** file and swap out 501 for your own.

And this **once**:
```sh
docker compose up airflow-init
```
If the exit code is 0 then it's all good.

### Mode

Demo: don't change anything.

Production : 
    First, put your Aviation Edge API in .aviationedge/api.txt.
    Then, in dags/dag.py, change `PRODUCTION_MODE = True`.

### Running

```sh
docker compose up
```

Instead of doing `docker compose up airflow-init` followed by `docker compose up`, you can start `.\start.bat`.


You can now connect to [localhost:8080](http://localhost:8080/) to access the airflow dashboard, user and password are `airflow`.


Now you must add a connection to the postgres SQL database. Navigate To the Admin -> Connections menu, then click the blue + button to add a new connection.

Fill in the form like in the image ! (airflow/assets/postgres_connection.png)

## Environment info
| Service  | Address:Port           | Image        |
| :------- | ---------------------- | ------------ |
| postgres | http://localhost:5432/ | postgres:13  |
| airflow  | http://localhost:8080/ |              |
| jupyter  | http://localhost:8888/ |              |
| redis    | http://localhost:6379/ | redis:latest |


Connect to postgres container: `docker exec -it airflow-postgres-1 psql -U airflow -d airflow`
