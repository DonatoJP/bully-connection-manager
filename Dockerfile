FROM python:3.8 as leader

WORKDIR /app
RUN apt-get -q update && apt-get -qy install netcat
RUN wget 'https://raw.githubusercontent.com/eficode/wait-for/master/wait-for'

COPY . .
ENTRYPOINT [ "/bin/sh" ]