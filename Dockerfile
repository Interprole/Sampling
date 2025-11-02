FROM python:3.11-slim

WORKDIR /code

COPY requirements.txt requirements.txt

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

ENV FLASK_APP app.py

EXPOSE 5002

CMD ["flask", "run", "--host", "0.0.0.0", "-p", "5002"]