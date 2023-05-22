FROM python:3.11.0

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt update; apt install -y \
    netcat \
    gettext \
    && pip install --no-cache-dir --upgrade pip

COPY requirements.txt requirements.txt
COPY entrypoint.sh entrypoint.sh

COPY . .

# Здорово, что работа процесса сервиса организована под отдельным пользователем
# и группой
RUN groupadd -r app && useradd -d /opt/app -r -g app app \
    && chown app:app -R /opt/app

RUN pip install --no-cache-dir -r requirements.txt

USER app


ENTRYPOINT ["./entrypoint.sh"]