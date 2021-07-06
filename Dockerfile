FROM emanuelbaq/pyspark

RUN apt-get update

RUN apt-get install -y libpq-dev python3-dev
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN apt-get install -y unixodbc-dev
RUN apt-get install -y build-essential libssl-dev libffi-dev python3-dev
RUN pip3 install pyodbc
RUN pip3 install pandas
RUN pip3 install pymongo
RUN pip3 install boto3
RUN pip3 install pandasql
RUN pip install psycopg2
