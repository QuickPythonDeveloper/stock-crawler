FROM python:3.9.6-buster

RUN apt update && apt install python3-pip python3-dev supervisor -y

RUN mkdir -p /src/
WORKDIR /src

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .

CMD ["python3", "-m", "src.main"]