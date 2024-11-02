# Cluebase 2.0

### Dev Setup

#### Python setup

`python3.10 -m venv venv`

`pip install -r requirements.txt`

#### Login to Prefect Cloud:

`prefect cloud login`

#### Login to AWS:

`aws configure`

Alternatively, create the credentials file yourself, at `~/.aws/credentials`

```
[default]
region=us-east-1
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY```