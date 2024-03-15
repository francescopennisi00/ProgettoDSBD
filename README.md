# DSBD Project
Distributed Systems and Big Data Project 2024

## Table of Contents
- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Introduction

The system in question is built following a microservices architectural pattern.
Microservices are packaged in special containers making use of Docker containerization technology,
which allows the microservices to be collected and isolated in complete runtime environments equipped with all the necessary files for execution,
to ensure portability to any infrastructure (hardware and software) that is Docker-enabled.
The objective of the system is to allow registered users to indicate, for each location of interest,
weather parameters, such as the maximum temperature or the possible presence of rain.
These parameters will be monitored by the system itself, and if the specified weather conditions are violated,
users will be notified by e-mail.
For more details we recommend to read documentation. [Documentation.](https://github.com/francescopennisi00/ProgettoDSBD/blob/main/docs/RelazioneDSBDGenovesePennisi2024.pdf)

## Installation

### Prerequisites

- Docker
- Kubernetes cluster
- Kind
- Kubectl

### Steps

-       git clone https://github.com/francescopennisi00/ProgettoDSBD.git
- Secrets have to be created in /docker directory <br>
    #### How to create Secrets in Docker
    In order to run application on Docker, you need to create text files with your secrets and put them in /docker directory.
    You have to name them in this way:
  - admin_psw.txt
  - apikey.txt
  - app_password.txt
  - email.txt 
  - notifier_DB_root_psw.txt
  - SLA_DB_root_psw.txt
  - um_DB_root_psw.txt
  - wms_DB_root_psw.txt
  - worker_DB_root_psw.txt <br> <br>

  The last five must contain the password of root user to the associated databases. <br>
  To get the weather forecast you need to use the services offered by OpenWeather,
  so we recommend that you register at this link (https://openweathermap.org/api) to free service option and get your
  API key, that you have to put it in the secret apikey.txt.<br>
  The secret email.txt must contain the email that you want to use, you have to use gmail like provider.<br>
  The secret app_password.txt must contain the password that gmail provides to your account in order to be used
  in applications such as this one. (It is not the password with which you access to your gmail account). <br>
  The secret admin_psw.txt must contain the password that you want to use in order to access
  at Service Level Agreement Manager.
    #### How to create Secrets in Kubernetes
    In order to run application on Kubernetes, you need to create Secret object with your secrets and put them in /kubernetes directory.
    You have to name them in this way:
  - admin-password.yaml
  - apikey.yaml
  - app_password.yaml
  - email.yaml
  - notifier_DB_root_psw.yaml
  - SLA_DB_root_psw.yaml
  - um_DB_root_psw.yaml
  - wms_DB_root_psw.yaml
  - worker_DB_root_psw.yaml <br> <br>

  The meaning of these secrets is the same as those we exposed about Secrets in Docker. [Secrets in Docker.](#how-to-create-secrets-in-docker) <br>
  Here there is an example of one Secret .yaml file: <br>

        apiVersion: v1
        kind: Secret
        metadata:
        name: notifier-db-root-psw
        namespace: weather-app
        type: Opaque
        stringData:
        psw: "your psw"
    The others secrets are similar to this one, the only differences are the name anche the value of psw.
<br><br>
- Modify your /etc/hosts file in Linux or \Windows\System32\drivers\etc\hosts file in Windows and insert the following line:

            127.0.0.1 localhost weather.com

- Run application
    #### How to run application on Docker
    Open a terminal and go to the project directory and then in ./docker directory. Then execute <br>

            docker compose up
    #### How to run application on Kubernetes
    - Installing Kind <br>
    See [install instructions](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) for more details. <br><br>
    - Creating a cluster<br>
    Once installed you can create a cluster with the following command executed from the directory /kind:

            kind create cluster --name weather --config config.yaml
    - Apply all the .yaml files with kubectl commands execute from the /kubernetes directory:

            kubectl apply -f namespace.yaml
            kubectl apply -f ./prometheus
            kubectl apply -f .
            kubectl apply -f ingress.yaml
## Usage
 
We recommend to use POSTMAN as client in order to interact with the system by REST API. <br>
For more information on the meaning and usefulness of endpoints we recommend to read documentation. [Documentation.](https://github.com/francescopennisi00/ProgettoDSBD/blob/main/docs/RelazioneDSBDGenovesePennisi2024.pdf)
<br> If you want to use your application in Docker then you have to use this base endpoint:
        
            http://weather.com:8080
If you want to use your application in Kubernetes then you have to use this base endpoint:

            http://weather.com

### Endpoints
You have to insert the request body in the <i>Pre-request Script</i> section and select <i>raw</i> in the
<i>Body</i> section and use JSON format.
- /register POST

            //In this way we fill the request body
            const timestampInNanoseconds = Date.now() * 1e6;
            pm.environment.set("timestampInNanoseconds", timestampInNanoseconds);
            const requestBody = {
            "timestamp_client": timestampInNanoseconds,
            "email": "your_email@gmail.com",
            "psw":"your_psw"
            };
            pm.request.body.raw = JSON.stringify(requestBody);
- /login POST

            //In this way we fill the request body
            const timestampInNanoseconds = Date.now() * 1e6;
            pm.environment.set("timestampInNanoseconds", timestampInNanoseconds);
            const requestBody = {
            "timestamp_client": timestampInNanoseconds,
            "email": "your_email@gmail.com",
            "psw":"your_psw"
            };
            pm.request.body.raw = JSON.stringify(requestBody);
- /delete_account POST

            //In this way we fill the request body
            const timestampInNanoseconds = Date.now() * 1e6;
            pm.environment.set("timestampInNanoseconds", timestampInNanoseconds);
            const requestBody = {
            "timestamp_client": timestampInNanoseconds,
            "email": "your_email@gmail.com",
            "psw":"your_psw"
            };
            pm.request.body.raw = JSON.stringify(requestBody);
For next endpoints you have to put the JWT Token in the HTTP Header, 
with Authorization as key and "Bearer your_JWT_token" as value.
<br>
- /update_rules POST

            //In this way we fill the request body
            const timestampInNanoseconds = Date.now() * 1e6;
            pm.environment.set("timestampInNanoseconds", timestampInNanoseconds);
            const requestBody = {
            "timestamp_client": timestampInNanoseconds,
            "trigger_period": trigger_period_chosen,
            "location": ["location_name", latitude, longitude, "country_code", "state_code"],
            "max_temp" : your value or "null" if you are not interested,
            "min_temp": your value or "null" if you are not interested,
            "max_humidity": your value or "null" if you are not interested,
            "min_humidity": your value or "null" if you are not interested,
            "max_pressure": your value or "null" if you are not interested,
            "min_pressure": your value or "null" if you are not interested,
            "max_wind_speed": your value or "null" if you are not interested,
            "min_wind_speed": your value or "null" if you are not interested,
            "wind_direction": your value or "null" if you are not interested,
            "rain": your value or "null" if you are not interested,
            "snow": your value or "null" if you are not interested,
            "max_cloud": your value or "null" if you are not interested,
            "min_cloud": your value or "null" if you are not interested
            };
            pm.request.body.raw = JSON.stringify(requestBody);
- /show_rules POST

            //In this way we fill the request body
            const timestampInNanoseconds = Date.now() * 1e6;
            pm.environment.set("timestampInNanoseconds", timestampInNanoseconds);
            const requestBody = {
            "timestamp_client": timestampInNanoseconds
            };
            pm.request.body.raw = JSON.stringify(requestBody);
- /update_rules/delete_user_constraints_by_location POST

            //In this way we fill the request body
            const timestampInNanoseconds = Date.now() * 1e6;
            pm.environment.set("timestampInNanoseconds", timestampInNanoseconds);
            const requestBody = {
            "timestamp_client": timestampInNanoseconds,
            "location": ["location_name", latitude, longitude, "country_code", "state_code"]
            };
            pm.request.body.raw = JSON.stringify(requestBody);
From now on you don't need to use the <i> Pre-request Script </i>, but you can make the request directly from the <i> Body</i> section.<br>
No Authorization Header is required for the next endpoint.<br>
- /adminlogin POST

            {
            "email": "your_admin_email@gmail.com",
            "psw":"your_admin_psw"
            }
For next endpoints you have to put the JWT Token in the HTTP Header,
with Authorization as key and "Bearer your_JWT_token" as value.
<br>
- /SLA_update_metrics POST

            {
            "metric_name":[min_target_value, max_target_value, seasonality_period]
            }
- /SLA_delete_metrics POST

            {
            "metrics":["metric_name1", "metric_name2", "metric_name3"]
            }
- /SLA_metrics_status GET
- /SLA_metrics_violations GET
- /SLA_forecasting_violations?minutes=number_of_minutes_chosen&metric_name=desidered_metric_name GET
<br><br>

The endpoints explained refer to the case 
where the application is running with Kubernetes.<br>
If the application is running with Docker the endpoints change a little bit, in this way:
- /usermanager/register
- /usermanager/login
- /usermanager/delete_account
- /wms/update_rules
- /wms/show_rules
- /wms/update_rules/delete_user_constraints_by_location
- /sla/adminlogin
- /sla/SLA_update_metrics
- /sla/SLA_delete_metrics
- /sla/SLA_metrics_status
- /sla/SLA_metrics_violations
- /sla/SLA_forecasting_violations?minutes=number_of_minutes_chosen&metric_name=desidered_metric_name

### Accessing to Prometheus
From your browser:

            http://localhost:9090
## Contributing
We welcome contributions from the community! If you'd like to contribute, please follow these steps:

1. Fork, then clone the repo:
2. Create a new branch: `git checkout -b feature-branch`
3. Make your changes
4. Push to your fork and submit a pull request

## License
This project is licensed under the terms of the MIT license. See [LICENSE](LICENSE) for more details.
