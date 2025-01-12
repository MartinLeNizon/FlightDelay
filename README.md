# DataEng 2024 Template Repository

![Insalogo](./images/logo-insa_0.png)

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

Students: **Jean-Christophe Schmitt & Martin Nizon-Deladoeuille**

### Usage

1. Get your computer's user id by typing in `id -u` in your bash terminal. As a window user you will have to run this command in your WSL terminal as it will not work on CMD or powershell.
2. Make sure to update the `AIRFLOW_UID` in the .env file to the ID you obtained.
3. Build and run the environment by executing `start.bat`.
5. You can now connect to [localhost:8080](http://localhost:8080/) to access the airflow dashboard, user and password are `airflow`.
6. Now you must add a connection to the postgres SQL database. Navigate To the Admin -> Connections menu, then click the blue + button to add a new connection.
7. Fill in the form like in the image ![](airflow/assets/postgres_connection.png)
## Environment info
| Service  | Address:Port           | Image        |
| :------- | ---------------------- | ------------ |
| postgres | http://localhost:5432/ | postgres:13  |
| airflow  | http://localhost:8080/ |              |
| jupyter  | http://localhost:8888/ |              |
| redis    | http://localhost:6379/ | redis:latest |


### Abstract

## Datasets Description 

## Queries 

## Requirements

## Note for Students

* Clone the created repository offline;
* Add your name and surname into the Readme file and your teammates as collaborators
* Complete the field above after project is approved
* Make any changes to your repository according to the specific assignment;
* Ensure code reproducibility and instructions on how to replicate the results;
* Add an open-source license, e.g., Apache 2.0;
* README is automatically converted into pdf

