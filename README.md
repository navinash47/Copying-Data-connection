# data-connection

Data connections for LLM.

See the [playbook](https://confluence.bmc.com/pages/viewpage.action?spaceKey=SolRD&title=Playbook+for+Deployment+of+HelixGPT)
to learn more about configuring and using the Helix GPT solution.

# Dependency Isolation (virtualenv)

* Set up your virtual environment

  ` virtualenv -p python3 venv `

####

* Activate your virtual environment for windows

  ` source ./venv/Scripts/activate `

* Activate your virtual environment for mac
  ` source ./venv/bin/activate `

####

* To deactivate your environment run this command :

  ` deactivate`

# Dependency Installation (pip)

* If you want to see your libraries, you can list them using :

  ` pip list `

####

* If you want to update your pip tool you can run this command :

  ` python -m pip install --upgrade pip `

####

To install all the dependencies of the project, run this command:

    pip install -r requirements.txt

To force the upgrade of a given dependencies:

    pip install --upgrade requests

To regenerate the `requirements.txt` file:

    pip freeze > requirements.txt

# Launching the application

## Precondition: Helix GPT Manager

The Helix GPT Manager component (https://github.bmc.com/HelixGPT/config-manager) needs to be deployed and accessible to
Data Connection. Data Connection will persist its jobs there, read its configuration from there, etc.

- Secure a Helix platform environment.
- Deploy Helix GPT Manager on it.
- Use the URL and credentials of that environment for the following settings:
  - `INNOVATION_SUITE_URL`
  - `INNOVATION_SUITE_USER`
  - `INNOVATION_SUITE_PASSWORD`

Note that pointing several data-connection apps to a single Innovation Suite instance isn't supported.


## Startup Configuration

The application configuration is provided via a `.env` file present in the execution folder. Copy the example and
customize it to your setup:

```
cp .env.example .env
```

Then, edit `.env`.


## Start Command

* In order to start the project you have to execute this command

  `uvicorn main:app --port=8000 --reload --app-dir src`


# Run OpenSearch for development

https://opensearch.org/docs/latest/install-and-configure/install-opensearch/docker/

This will run OpenSearch by itself (without OpenSearch Dashboards):

```shell
docker pull opensearchproject/opensearch:2

# Start OpenSearch
docker run --rm -d -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" opensearchproject/opensearch:2

# check OpenSearch is up and running
curl https://127.0.0.1:9200 -ku 'admin:<your_password_here>'
```

The [page linked above](https://opensearch.org/docs/latest/install-and-configure/install-opensearch/docker/)
also gives the default credentials.

# Run with Docker Compose

Start data-connection and OpenSearch together in the background:
```shell
docker-compose up -d
```

Stop and remove containers and networks:
```shell
docker-compose down
```

## MacOs - Rancher Desktop

By default, the Rancher Desktop might be too small and not configured correctly to run OpenSearch.
* Increase the VM memory allowance to 5GB minimum.
* Execute the following command:
`rdctl shell sudo sysctl -w vm.max_map_count=262144`
* Then, restart OpenSearch: `docker-compose restart opensearch`

C.f. https://apple.stackexchange.com/questions/448632/memory-error-when-running-elasticsearch-on-docker-macos-monterey-12-3

## MacOs - Stable host IP

The Docker containers execute in the Rancher Desktop VM, which is their actual host.
However, it is often useful to be able to reference the MacOs host reliably, either to access the port exposed by
Docker Compose or to expose some other service, which runs directly on MacOs. You can capture your IP address with
`ifconfig`, but it can be more convenient to define a loopback IP address:
```shell
sudo ifconfig lo0 alias 192.168.0.1
```
Then, you can use `192.168.0.1` like in https://admin:<your_password_here>@192.168.0.1:9002/_aliases.
To make this permanent, see [this article](https://medium.com/@david.limkys/permanently-create-an-ifconfig-loopback-alias-macos-b7c93a8b0db).

# RKM Integration
## Configuration

As usual, settings can be specified via environment variables (which can translate to the `.env` file for the Docker Compose setup)
or via the `.env` file (relative to Uvicorn's execution dir).

Example for the [DWP 21.3 QA Primary env RKM](https://confluence.bmc.com/display/MyService/21.3.xx+QA+Primary+Env):
```shell
RKM_URL=https://dwppune03-dwpc.helixsolqa.bmc.com
RKM_USER=hannah_admin
RKM_PASSWORD=<see environment page>
RKM_VERIFY_CERTIFICATES=False
```

## Indexing RKM articles via the API

All the published articles (for supported templates):
```
POST /api/v1.0/jobs
{
  "datasource": "RKM"
}
```

One article by display "Article ID" (as labelled in the UI; form field `DocID` / `302300507`):
```
POST /api/v1.0/jobs
{
  "datasource": "RKM",
  "docDisplayId": "KBA90000074"
}
```

One article by GUID (form field `InstanceId` / `179`)
```
POST /api/v1.0/jobs
{
  "datasource": "RKM",
  "docId": "KBGAA5V0HI5R3ANAK7TBLT3IBFQJC8"
}
```


# Project Structure

The structure of this project is

```
data-connection
├── src
│   ├── users
│   │   ├── router.py
│   │   ├── schemas.py  # pydantic models
│   │   ├── models.py  # db models
│   │   ├── dependencies.py
│   │   ├── config.py  # local configs
│   │   ├── constants.py
│   │   ├── exceptions.py
│   │   ├── service.py
│   │   └── utils.py
│   ├── config.py  # global configs
│   ├── models.py  # global models
│   ├── exceptions.py  # global exceptions
│   ├── database.py  # db connection related stuff
│   └── main.py
├── tests/
├── requirements
├── .env
├── .gitignore
```

1. Store all domain directories inside src folder
    1. src/ - highest level of an app, contains common models, configs, and constants, etc.
    2. src/main.py - root of the project, which inits the FastAPI app
2. Each package has its own router, schemas, models, etc ( users folder is only an example)
    1. router.py - is a core of each module with all the endpoints
    2. schemas.py - for pydantic models
    3. models.py - for db models
    4. service.py - specific business logic
    5. dependencies.py - router dependencies , jwt token and other validations
    6. constants.py - module specific constants and error codes
    7. config.py - e.g. env vars, this is going to read environment variables that we declare on `.env`
    8. utils.py - non-business logic functions, e.g. response normalization, data enrichment, etc.
    9. exceptions.py - module specific exceptions, e.g. PostNotFound, InvalidUserData
