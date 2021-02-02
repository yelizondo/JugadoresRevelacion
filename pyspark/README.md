## Repository for course: database II
This readme helps you to perform the intended labs using pyspark. Image courtesy of phD. Juan Esquivel

### docker related  
To build the image and run it 
```
docker build . -t pyspark

docker run -it --rm -v "C:\Users\Mau\Documents\TEC\2020\Semestre II\Bases II\Progra II\JugadoresRevelacion\pyspark\examples":/src pyspark bash
docker run -it --rm -v /Users/desarrollador/Projects/JugadoresRevelacion/pyspark/examples:/src pyspark bash
```

### pyspark related
To run the examples use the spark-submit command, for example:

```
spark-submit aggregate.py
cp 2017predictions.json ../../viz/files/e65374209781891f37dea1e7a6e1c5e020a3009b8aedf113b4c80942018887a1176ad4945cf14444603ff91d3da371b3b0d72419fa8d2ee0f6e815732475d5de
```