## Repository for course: database II
This readme helps you to perform the intended labs using pyspark. Image courtesy of phD. Juan Esquivel

### docker related  
To build the image and run it 
```
docker build . -t pyspark

docker run -it --rm -v "C:\Users\Mau\Documents\TEC\2020\Semestre II\Bases II\Progra II\JugadoresRevelacion\pyspark\examples":/src pyspark bash
```

### pyspark related
To run the examples use the spark-submit command, for example:

```
spark-submit aggregate.py
```