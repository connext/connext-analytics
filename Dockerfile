FROM python:3.11.4

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

WORKDIR /app
COPY Pipfile.lock Pipfile ./

# Building deps
RUN pip install pipenv
RUN pipenv install --dev --system --deploy

EXPOSE 8080

COPY . /app

CMD ["uvicorn", "src.pipelines:app", "--host", "0.0.0.0", "--port", "8080", "--reload", "--interface", "auto"]
