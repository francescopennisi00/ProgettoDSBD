# DSBD Project
Project of Distributed Systems and Big Data 2024

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
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

## Features

- 
- Tracing demo with Jaeger and OpenTelemetry.
- WIP

## Installation

### Prerequisites

- Python 3.10 or higher
- Docker
- Kubernetes cluster
- Kind

### Steps

-       git clone https://github.com/francescopennisi00/ProgettoDSBD.git
- Secrets have to be created in /docker directory <br>
    #### How to create Secrets in Docker
    In order to run application on Docker, you need to create text files with your secrets and put them in /docker directory.
    You have to name them in this way:
  - apikey.txt
  - app_password.txt
  - email.txt 
  - notifier_DB_root_psw.txt
  - SLA_DB_root_psw.txt
  - um_DB_root_psw.txt
  - wms_DB_root_psw.txt
  - worker_DB_root_psw.txt <br> <br>

  The lasts five must contain the password of root user to the databases. <br>
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

#### How to run application on Docker
docker compose up 

## Usage
 
WIP

## Contributing
We welcome contributions from the community! If you'd like to contribute, please follow these steps:

1. Fork, then clone the repo:
2. Create a new branch: `git checkout -b feature-branch`
3. Make your changes
4. Push to your fork and submit a pull request

## License
This project is licensed under the terms of the MIT license. See [LICENSE](LICENSE) for more details.