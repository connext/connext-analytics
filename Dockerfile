FROM python:3.11.4

WORKDIR /app
COPY Pipfile.lock Pipfile ./

# set env variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1


# Building deps
RUN pip install pipenv
RUN pipenv install --dev --system --deploy

COPY . /app