FROM nginx:1.24-bullseye
RUN apt update &&  \
    apt autoclean && apt -f install && apt-get install -y nginx-extras vim

COPY ../configs/nginx.conf /etc/nginx/conf.d/nginx.conf

