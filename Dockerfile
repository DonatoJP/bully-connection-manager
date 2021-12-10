FROM python:3.9 as leader

WORKDIR /app
RUN apt-get -q update && apt-get -qy install netcat
RUN wget 'https://raw.githubusercontent.com/eficode/wait-for/master/wait-for'

RUN pip install docker

COPY . .
ENTRYPOINT [ "/bin/sh" ]