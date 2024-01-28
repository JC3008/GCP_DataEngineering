FROM python:3.9-bullseye

RUN  pip install --upgrade pip 

RUN mkdir -p /workspaces/app/elt_script data_generator sources SQL

COPY /elt_script /workspaces/app/elt_script
COPY /data_generator /workspaces/app/data_generator 
COPY /sources /workspaces/app/sources 
COPY /SQL /workspaces/app/SQL 
COPY elt_script/vendas-de-412318-2cdd56112d35.json /workspaces/app/elt_script

WORKDIR /workspaces/app
RUN pip install -r /workspaces/app/elt_script/requirements.txt

ENV PYTHONPATH=/usr/local/lib/python3.9/dist-packages

EXPOSE 8888
# ENV PYTHONPATH=/usr/local/bin/python

# CMD ["python"]

# first test for CI