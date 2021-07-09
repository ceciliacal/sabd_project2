# sabd_project2

Per eseguire l'applicazione effettuando il deploy sul cluster di kafka e flink, è suffinte  eseguire il comando "./start-docker.sh" all'interno della cartella "docker". 
Per effettuare il submit del job con Flink, aprire un altro terminale e digirare il seguente comando:

> sudo docker exec -t -i jobmanager /bin/bash "flink run -c connectionToKafka.MyConsumer ./queries/flink.jar". 
>
In alternativa, si può prima eseguire "sudo docker exec -t -i jobmanager /bin/bash" e, una volta entrati nella shell del container del jobmanager, eseguire "flink run -c connectionToKafka.MyConsumer ./queries/flink.jar".